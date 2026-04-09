from __future__ import annotations

from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
from constructs import Construct

from infrastructure.config import ProjectConfig
from infrastructure.user_data import build_user_data


class ComputeStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: ProjectConfig,
        vpc: ec2.IVpc,
        model_bucket: s3.IBucket,
        daily_feature_table: dynamodb.ITable,
        audit_table: dynamodb.ITable,
        sync_status_table: dynamodb.ITable,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        security_group = ec2.SecurityGroup(
            self,
            "Ec2SecurityGroup",
            vpc=vpc,
            description="Allow HTTP access and restricted SSH access",
            allow_all_outbound=True,
        )
        security_group.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(80), "HTTP")
        security_group.add_ingress_rule(
            ec2.Peer.ipv4(config.allowed_ssh_cidr),
            ec2.Port.tcp(22),
            "SSH",
        )

        role = iam.Role(
            self,
            "Ec2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
            ],
        )

        model_bucket.grant_read(role)
        model_bucket.grant_put(role)
        daily_feature_table.grant_read_write_data(role)
        audit_table.grant_write_data(role)
        sync_status_table.grant_read_write_data(role)

        app_source_asset = s3_assets.Asset(
            self,
            "PrecioWattAppSource",
            path="preciowatt_v3_flask",
        )
        app_source_asset.grant_read(role)

        user_data_script = build_user_data(
            config,
            bucket_name=model_bucket.bucket_name,
            daily_feature_table_name=daily_feature_table.table_name,
            audit_table_name=audit_table.table_name,
            sync_status_table_name=sync_status_table.table_name,
            app_asset_bucket_name=app_source_asset.s3_bucket_name,
            app_asset_object_key=app_source_asset.s3_object_key,
        )

        machine_image = ec2.MachineImage.latest_amazon_linux2023()
        user_data = ec2.UserData.custom(user_data_script)

        instance_kwargs = dict(
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            instance_type=ec2.InstanceType(config.instance_type),
            machine_image=machine_image,
            security_group=security_group,
            role=role,
            user_data=user_data,
            require_imdsv2=True,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        8,
                        encrypted=True,
                        delete_on_termination=True,
                    ),
                )
            ],
        )
        if config.key_name:
            instance_kwargs["key_name"] = config.key_name

        instance = ec2.Instance(
            self,
            "AppInstance",
            **instance_kwargs,
        )

        eip = ec2.CfnEIP(
            self,
            "ElasticIpResource",
            domain="vpc",
        )
        ec2.CfnEIPAssociation(
            self,
            "ElasticIpAssociation",
            allocation_id=eip.attr_allocation_id,
            instance_id=instance.instance_id,
        )

        CfnOutput(self, "ElasticIpOutput", value=eip.ref)
        CfnOutput(self, "InstanceId", value=instance.instance_id)
        CfnOutput(self, "AppUrl", value=f"http://{eip.ref}/")
        CfnOutput(self, "ModelBucketName", value=model_bucket.bucket_name)
        CfnOutput(self, "DailyFeatureTableName", value=daily_feature_table.table_name)
        CfnOutput(self, "AuditTableName", value=audit_table.table_name)
        CfnOutput(self, "SyncStatusTableName", value=sync_status_table.table_name)
