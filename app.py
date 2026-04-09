#!/usr/bin/env python3
import os

import aws_cdk as cdk

from infrastructure.compute_stack import ComputeStack
from infrastructure.config import load_project_config
from infrastructure.data_stack import DataStack
from infrastructure.network_stack import NetworkStack


app = cdk.App()
config = load_project_config(app)

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=config.aws_region,
)

network_stack = NetworkStack(
    app,
    f"{config.project_name}-network",
    config=config,
    env=env,
)

data_stack = DataStack(
    app,
    f"{config.project_name}-data",
    config=config,
    env=env,
)

compute_stack = ComputeStack(
    app,
    f"{config.project_name}-compute",
    config=config,
    vpc=network_stack.vpc,
    model_bucket=data_stack.model_bucket,
    daily_feature_table=data_stack.daily_feature_table,
    audit_table=data_stack.audit_table,
    sync_status_table=data_stack.sync_status_table,
    env=env,
)

compute_stack.add_dependency(network_stack)
compute_stack.add_dependency(data_stack)

app.synth()
