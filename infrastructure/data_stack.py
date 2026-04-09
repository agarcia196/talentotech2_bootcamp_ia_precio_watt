from __future__ import annotations

from aws_cdk import CfnOutput, RemovalPolicy, Stack, aws_dynamodb as dynamodb, aws_s3 as s3
from constructs import Construct

from infrastructure.config import ProjectConfig


class DataStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, config: ProjectConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.model_bucket = s3.Bucket(
            self,
            "ModelBucket",
            bucket_name=f"{config.model_bucket_prefix}-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.daily_feature_table = dynamodb.Table(
            self,
            "DailyFeatureTable",
            partition_key=dynamodb.Attribute(
                name="record_type",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="date",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=False,
        )

        self.audit_table = dynamodb.Table(
            self,
            "PredictionAuditTable",
            partition_key=dynamodb.Attribute(
                name="request_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=False,
        )

        self.sync_status_table = dynamodb.Table(
            self,
            "SyncStatusTable",
            table_name=f"{config.project_name}-{config.sync_status_table_name}",
            partition_key=dynamodb.Attribute(
                name="sync_name",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=False,
        )

        CfnOutput(self, "ModelBucketName", value=self.model_bucket.bucket_name)
        CfnOutput(self, "DailyFeatureTableName", value=self.daily_feature_table.table_name)
        CfnOutput(self, "AuditTableName", value=self.audit_table.table_name)
        CfnOutput(self, "SyncStatusTableName", value=self.sync_status_table.table_name)
