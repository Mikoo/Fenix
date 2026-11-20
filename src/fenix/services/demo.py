"""Laboratorio acotado de recursos. Integrante 2 prepara, integrante 6 verifica."""
from fenix.config.settings import client
from fenix.utils.state import load, save
from fenix.utils.validation import identifier


def setup_demo(group: str) -> dict:
    """Prepara una instancia mock y un bucket a partir de identificadores estables.

    Args:
        group: ID validado del grupo; determina prefijos de los recursos.
    Returns:
        Inventario con aliases node-01 y bucket-01 y sus IDs reales del emulador.
    Raises:
        ClientError: Si el emulador no soporta una operación o falla su ejecución.
        NotImplementedError: Mientras falte TODO I2-C.
    Side Effects:
        Crea recursos de laboratorio etiquetados y data/inventory.json.
    Notes:
        Reutilizar por tags antes de crear y guardar IDs después de cada creación.
        Obtener una ImageId con describe_images; no copiar una AMI de AWS real.
    """
    # TODO I2-C: validar group; localizar VPC predeterminada mediante describe_vpcs.
    # Crear/reusar SG fenix-{group}-sg con tag ManagedBy=ppa2 y GroupId=group.
    # Usar describe_images y registrar la ImageId mock elegida en inventory.json.
    # Crear/reusar instancia por tags Project=fenix/GroupId/Name; ClientToken estable.
    # run_instances: MinCount=MaxCount=1, tipo t3.micro y SecurityGroupIds=[sg_id].
    # Crear/reusar bucket fenix-{group}-demo; región us-east-1 para este laboratorio.
    # Aplicar tags ManagedBy=ppa2, Project=fenix, GroupId=group y Owner=group.
    # Publicar inventario solo con recursos cuya propiedad haya sido verificada.
    from botocore.exceptions import ClientError
    identifier(group) # Valida el formato del grupo
    
    ec2 = client("ec2")
    s3 = client("s3")
    
    # Localizar VPC predeterminada
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        raise RuntimeError("No se encontró VPC predeterminada. Revisar entorno de LocalStack.")
    vpc_id = vpcs[0]["VpcId"]
    
    # Obtener una ImageId mock
    images = ec2.describe_images(Owners=["amazon"])["Images"]
    if not images:
        raise RuntimeError("No hay AMI mock disponible en LocalStack.")
    image_id = images[0]["ImageId"]
    
    # Crear o reusar Security Group
    sg_name = f"fenix-{group}-sg"
    sgs = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [sg_name]}])["SecurityGroups"]
    if sgs:
        sg_id = sgs[0]["GroupId"]
    else:
        sg_id = ec2.create_security_group(
            GroupName=sg_name, Description=f"SG para {group}", VpcId=vpc_id
        )["GroupId"]
        ec2.create_tags(Resources=[sg_id], Tags=[
            {"Key": "ManagedBy", "Value": "ppa2"}, {"Key": "GroupId", "Value": group}
        ])
        
    # Crear o reusar Instancia EC2
    instance_name = f"fenix-{group}-node-01"
    reservations = ec2.describe_instances(Filters=[
        {"Name": "tag:Project", "Values": ["fenix"]},
        {"Name": "tag:GroupId", "Values": [group]},
        {"Name": "tag:Name", "Values": [instance_name]}
    ])["Reservations"]
    
    if reservations:
        instance_id = reservations[0]["Instances"][0]["InstanceId"]
    else:
        instance_id = ec2.run_instances(
            ImageId=image_id,
            MinCount=1, MaxCount=1,
            InstanceType="t3.micro",
            SecurityGroupIds=[sg_id],
            ClientToken=f"fenix-{group}-node-01", # Idempotencia
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": instance_name},
                    {"Key": "ManagedBy", "Value": "ppa2"},
                    {"Key": "Project", "Value": "fenix"},
                    {"Key": "GroupId", "Value": group},
                    {"Key": "Owner", "Value": group}
                ]
            }]
        )["Instances"][0]["InstanceId"]
        
    # Crear o reusar Bucket S3
    bucket_name = f"fenix-{group}-demo"
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket_name) # us-east-1 no necesita configuración extra
        else:
            raise
            
    # Aplicar tags al Bucket
    s3.put_bucket_tagging(
        Bucket=bucket_name,
        Tagging={"TagSet": [
            {"Key": "ManagedBy", "Value": "ppa2"},
            {"Key": "Project", "Value": "fenix"},
            {"Key": "GroupId", "Value": group},
            {"Key": "Owner", "Value": group}
        ]}
    )
    
    # Armar y guardar el inventario
    inventory = {
        "group": group,
        "node-01": {
            "type": "ec2",
            "id": instance_id,
            "security_group_id": sg_id,
            "expected_tags": {"ManagedBy": "ppa2", "Project": "fenix", "GroupId": group, "Owner": group}
        },
        "bucket-01": {
            "type": "s3",
            "id": bucket_name,
            "expected_tags": {"ManagedBy": "ppa2", "Project": "fenix", "GroupId": group, "Owner": group}
        }
    }
    
    # Guardamos usando la utilidad provista
    save("data/inventory.json", inventory)
    return inventory


def inject_fault(alias: str) -> None:
    """Detiene el nodo o elimina una etiqueta del bucket de laboratorio.

    Args:
        alias: node-01 o bucket-01 presente en inventory.json.
    Returns:
        None cuando el fallo simulado se verifica mediante el SDK.
    Raises:
        ValidationError: Si el alias no pertenece al inventario.
        NotImplementedError: Mientras falte TODO I6-A.
    Side Effects:
        Modifica únicamente el recurso de prueba de ese grupo.
    """
    # TODO I6-A: resolver el alias en inventario; jamás recibir InstanceId libre.
    # node-01: stop_instances y comprobar stopped, con consultas limitadas.
    # bucket-01: quitar Owner conservando el resto de etiquetas.
    # Esta función fabrica el problema; no debe enviar el evento automáticamente.
    raise NotImplementedError("I6-A: inyectar y comprobar fallo")
