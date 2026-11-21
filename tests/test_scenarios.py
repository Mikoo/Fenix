"""Escenarios integrales de autorreparacion, worker SQS y redrive a DLQ."""
import json
import logging
import time
from unittest.mock import patch
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
import pytest
from fenix.services import remediation, worker
from fenix.utils.errors import RetryableError
from fenix.utils.state import load, save


def _localstack_ready() -> bool:
    """Comprueba si LocalStack responde en localhost:4566."""
    try:
        sqs = boto3.client(
            "sqs",
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(connect_timeout=1, read_timeout=2),
        )
        sqs.list_queues()
        return True
    except (BotoCoreError, ClientError, OSError):
        return False


_LOCALSTACK_AVAILABLE = _localstack_ready()


@pytest.mark.pendiente
def test_duplicate_is_idempotent(tmp_path, monkeypatch):
    """Comprueba que repetir un evento no repite recursos ni notificaciones."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    save(
        "data/inventory.json",
        {
            "node-01": {
                "type": "ec2",
                "id": "i-mock01",
                "expected_tags": {"GroupId": "grupo-01"},
            }
        },
    )
    save("data/events.json", {})
    event = {
        "schema_version": 1,
        "event_id": "evt-dup-001",
        "event_type": "instance.stopped",
        "resource_id": "node-01",
        "occurred_at": "2026-10-22T18:00:00Z",
        "simulate_failures": 0,
    }
    repair_calls = []
    notify_calls = []
    monkeypatch.setattr(
        "fenix.services.remediation.repair",
        lambda res, etype: repair_calls.append((res["id"], etype))
        or "repaired",
    )
    monkeypatch.setattr(
        "fenix.services.remediation.notify",
        notify_calls.append,
    )

    remediation.handle(event)
    first_ledger = load("data/events.json")
    assert first_ledger["evt-dup-001"]["repair_done"] is True
    assert first_ledger["evt-dup-001"]["notified"] is True

    remediation.handle(dict(event))
    second_ledger = load("data/events.json")
    assert second_ledger["evt-dup-001"]["attempts"] == 1
    assert len(repair_calls) == 1
    assert len(notify_calls) == 1


@pytest.mark.pendiente
def test_ack_is_after_success():
    """Verifica que un handler fallido jamas confirma ACK y uno sano si."""
    order: list[str] = []
    ack_calls: list[str] = []
    retry_calls: list[str] = []
    msg = {
        "MessageId": "m-1",
        "Body": '{"event_id":"e1","resource_id":"node-01"}',
        "ReceiptHandle": "rh-1",
        "Attributes": {"ApproximateReceiveCount": "1"},
    }
    logger = logging.getLogger("test_scenarios_ack")

    def _failing_handler(_event: dict) -> None:
        order.append("fail_handler")
        raise RetryableError("fallo simulado")

    def _ok_handler(_event: dict) -> None:
        order.append("ok_handler")

    with (
        patch("fenix.services.worker.receive", return_value=[msg]),
        patch(
            "fenix.services.worker.acknowledge",
            side_effect=lambda _u, rh: (
                order.append("ack"),
                ack_calls.append(rh),
            ),
        ),
        patch(
            "fenix.services.worker.retry_later",
            side_effect=lambda _u, m: retry_calls.append(m["ReceiptHandle"])
            or 2,
        ),
    ):
        worker.run("http://fake", _failing_handler, logger, once=True)
        assert not ack_calls
        assert retry_calls == ["rh-1"]

        worker.run("http://fake", _ok_handler, logger, once=True)
        assert ack_calls == ["rh-1"]
        assert order == ["fail_handler", "ok_handler", "ack"]


@pytest.mark.integracion
@pytest.mark.skipif(
    not _LOCALSTACK_AVAILABLE, reason="LocalStack no disponible"
)
def test_redrive_with_localstack():
    """Comprueba DLQ efectiva con RedrivePolicy real en LocalStack."""
    prefix = "fenix-test-dlq-"
    sqs = boto3.client(
        "sqs",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(connect_timeout=2, read_timeout=15),
    )
    dlq_url = sqs.create_queue(QueueName=prefix + "dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    redrive = json.dumps(
        {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 1}
    )
    main_url = sqs.create_queue(
        QueueName=prefix + "main",
        Attributes={"RedrivePolicy": redrive},
    )["QueueUrl"]
    try:
        body = '{"broken": true}'
        sqs.send_message(QueueUrl=main_url, MessageBody=body)
        deadline = time.time() + 120
        found = False
        while time.time() < deadline and not found:
            resp = sqs.receive_message(
                QueueUrl=main_url, MaxNumberOfMessages=1, WaitTimeSeconds=2
            )
            for msg in resp.get("Messages", []):
                sqs.change_message_visibility(
                    QueueUrl=main_url,
                    ReceiptHandle=msg["ReceiptHandle"],
                    VisibilityTimeout=0,
                )
            dlq_resp = sqs.receive_message(
                QueueUrl=dlq_url, MaxNumberOfMessages=1, WaitTimeSeconds=2
            )
            found = any(
                item["Body"] == body for item in dlq_resp.get("Messages", [])
            )
        assert found, "El mensaje corrupto no llego a la DLQ"
    finally:
        sqs.delete_queue(QueueUrl=main_url)
        sqs.delete_queue(QueueUrl=dlq_url)
