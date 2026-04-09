from __future__ import annotations

from aws_cdk import CfnOutput, Stack, aws_ec2 as ec2
from constructs import Construct

from infrastructure.config import ProjectConfig


class NetworkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, config: ProjectConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr(config.vpc_cidr),
            max_azs=1,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=26,
                )
            ],
            enable_dns_hostnames=True,
            enable_dns_support=True,
        )

        CfnOutput(self, "VpcId", value=self.vpc.vpc_id)
        CfnOutput(
            self,
            "PublicSubnetId",
            value=self.vpc.public_subnets[0].subnet_id,
        )
