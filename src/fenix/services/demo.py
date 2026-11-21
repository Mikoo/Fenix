"""Laboratorio acotado de recursos. Integrante 2 prepara, 6 verifica."""
import time
from botocore.exceptions import ClientError
from fenix.config.settings import client
from fenix.utils.errors import PermanentError, ValidationError
from fenix.utils.state import load, save
from fenix.utils.validation import identifier


def _ensure_security_group(ec2, group: str, vpc_id: str) -> str:
    """Crea o reutiliza el Security Group del laboratorio para el grupo."""
    sg_name = f"fenix-{group}-sg"
    sgs = ec2.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [sg_name]}]
    )["SecurityGroups"]
    if sgs:
        return sgs[0]["GroupId"]
    sg_id = ec2.create_security_group(
        GroupName=sg_name, Description=f"SG para {group}", VpcId=vpc_id
    )["GroupId"]
    ec2.create_tags(
        Resources=[sg_id],
        Tags=[
            {"Key": "ManagedBy", "Value": "ppa2"},
            {"Key": "GroupId", "Value": group},
        ],
    )
    return sg_id


def _ensure_instance(
    ec2, group: str, sg_id: str, image_id: str
) -> str:
    """Crea o reutiliza la instancia EC2 mock activa del laboratorio."""
    instance_name = f"fenix-{group}-node-01"
    reservations = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Project", "Values": ["fenix"]},
            {"Name": "tag:GroupId", "Values": [group]},
            {"Name": "tag:Name", "Values": [instance_name]},
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running", "stopping", "stopped"],
            },
        ]
    )["Reservations"]
    if reservations and reservations[0].get("Instances"):
        return reservations[0]["Instances"][0]["InstanceId"]
    return ec2.run_instances(
        ImageId=image_id,
        MinCount=1,
        MaxCount=1,
        InstanceType="t3.micro",
        SecurityGroupIds=[sg_id],
        ClientToken=f"fenix-{group}-node-01",
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": instance_name},
                    {"Key": "ManagedBy", "Value": "ppa2"},
                    {"Key": "Project", "Value": "fenix"},
                    {"Key": "GroupId", "Value": group},
                    {"Key": "Owner", "Value": group},
                ],
            }
        ],
    )["Instances"][0]["InstanceId"]


def _ensure_bucket(s3, group: str, expected_tags: dict[str, str]) -> str:
    """Crea o reutiliza el bucket S3 de laboratorio aplicando sus tags."""
    bucket_name = f"fenix-{group}-demo"
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket_name)
        else:
            raise
    s3.put_bucket_tagging(
        Bucket=bucket_name,
        Tagging={
            "TagSet": [
                {"Key": key, "Value": val}
                for key, val in expected_tags.items()
            ]
        },
    )
    return bucket_name


def setup_demo(group: str) -> dict:
    """Prepara una instancia mock y un bucket a partir de IDs estables.

    Args:
        group: ID validado del grupo; determina prefijos de los recursos.
    Returns:
        Inventario con aliases node-01 y bucket-01 y sus IDs del emulador.
    Raises:
        ClientError: Si el emulador no soporta una operacion o falla.
        RuntimeError: Si LocalStack no expone VPC default o AMI mock.
    Side Effects:
        Crea recursos de laboratorio etiquetados y data/inventory.json.
    """
    identifier(group)
    ec2 = client("ec2")
    s3 = client("s3")
    vpcs = ec2.describe_vpcs(
        Filters=[{"Name": "is-default", "Values": ["true"]}]
    )["Vpcs"]
    if not vpcs:
        raise RuntimeError(
            "No se encontro VPC predeterminada. Revisar LocalStack."
        )
    images = ec2.describe_images(Owners=["amazon"])["Images"]
    if not images:
        raise RuntimeError("No hay AMI mock disponible en LocalStack.")
    image_id = images[0]["ImageId"]
    sg_id = _ensure_security_group(ec2, group, vpcs[0]["VpcId"])
    instance_id = _ensure_instance(ec2, group, sg_id, image_id)
    expected_tags = {
        "ManagedBy": "ppa2",
        "Project": "fenix",
        "GroupId": group,
        "Owner": group,
    }
    bucket_name = _ensure_bucket(s3, group, expected_tags)
    inventory = {
        "group": group,
        "node-01": {
            "type": "ec2",
            "id": instance_id,
            "image_id": image_id,
            "security_group_id": sg_id,
            "expected_tags": dict(expected_tags),
        },
        "bucket-01": {
            "type": "s3",
            "id": bucket_name,
            "expected_tags": dict(expected_tags),
        },
    }
    save("data/inventory.json", inventory)
    return inventory


def _fault_node(resource_id: str) -> None:
    """Detiene la instancia mock y confirma estado stopped en 10 s."""
    ec2 = client("ec2")
    ec2.stop_instances(InstanceIds=[resource_id])
    for _ in range(10):
        response = ec2.describe_instances(InstanceIds=[resource_id])
        state = response["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state == "stopped":
            return
        time.sleep(1)
    raise PermanentError(
        "La instancia no llego a stopped en el tiempo acotado"
    )


def _fault_bucket(resource_id: str) -> None:
    """Elimina la etiqueta Owner del bucket conservando el resto."""
    s3 = client("s3")
    tagging = s3.get_bucket_tagging(Bucket=resource_id)
    current = tagging.get("TagSet", [])
    new_tags = [tag for tag in current if tag.get("Key") != "Owner"]
    if len(new_tags) == len(current):
        raise PermanentError("La etiqueta Owner ya estaba ausente")
    s3.put_bucket_tagging(Bucket=resource_id, Tagging={"TagSet": new_tags})


def inject_fault(alias: str) -> None:
    """Detiene el nodo o elimina una etiqueta del bucket de laboratorio.

    Args:
        alias: node-01 o bucket-01 presente en inventory.json.
    Raises:
        ValidationError: Si el alias no pertenece al inventario.
        PermanentError: Si el nodo no converge o falta la etiqueta.
    Side Effects:
        Modifica unicamente el recurso de prueba de ese grupo.
    """
    if alias not in ("node-01", "bucket-01"):
        raise ValidationError(f"Alias no permitido: {alias}")
    inventory = load("data/inventory.json")
    if not isinstance(inventory, dict) or alias not in inventory:
        raise ValidationError(f"Alias no presente en el inventario: {alias}")
    resource = inventory[alias]
    if not isinstance(resource, dict) or "id" not in resource:
        raise ValidationError(f"Inventario invalido para {alias}")
    resource_type = resource.get("type")
    resource_id = resource["id"]
    if alias == "node-01" and resource_type == "ec2":
        _fault_node(resource_id)
        return
    if alias == "bucket-01" and resource_type == "s3":
        _fault_bucket(resource_id)
        return
    raise ValidationError(f"Alias o tipo de recurso no soportado: {alias}")
