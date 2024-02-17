import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { AlbStack } from './alb-stack';
import { AnalyticsStack } from './analytics-stack';
import { BackendStack } from './backend-stack';
import { EcsClusterStack } from './ecs-cluster-stack';
import { EcsTaskExecutionRoleStack } from './ecr-exec-task-role';
import { InfluxDBStack } from './influxdb-stack';
import { IngestStack } from './ingest-stack';
import { KinesisStreamsStack } from './kinesis-stack';
import { VpcStack } from './vpc-stack';
import { WebSocketApiGatewayStack } from './api-gateway-stack';

export class ControlTowerStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpcStack = new VpcStack(this, 'VpcStack');

    const wsApigatewayStack = new WebSocketApiGatewayStack(
      this,
      'ApiGatewayStack'
    );

    const kinesisStack = new KinesisStreamsStack(
      this,
      'KinesisStack',
      wsApigatewayStack.webSocketApiGatewayStageProdArn,
      wsApigatewayStack.webSocketApiGatewayConnectionsUrl
    );

    const ecsCluster = new EcsClusterStack(
      this,
      'EcsClusterStack',
      vpcStack.vpc
    );

    const albStack = new AlbStack(this, 'AlbStack', vpcStack.vpc);

    const taskExecRoleStack = new EcsTaskExecutionRoleStack(
      this,
      'EcsTaskExecutionRoleStack',
      [ecsCluster.influxDBRepositoryName, ecsCluster.ingestRepositoryName]
    );

    const influxStack = new InfluxDBStack(
      this,
      'InfluxDbStack',
      vpcStack.vpc,
      ecsCluster.ecsCluster,
      taskExecRoleStack.role,
      albStack.influxDBAlbListener
    );

    const ingestStack = new IngestStack(
      this,
      'IngestStack',
      ecsCluster.ecsCluster,
      taskExecRoleStack.role
    );
    ingestStack.addDependency(kinesisStack);

    const analyticsStack = new AnalyticsStack(
      this,
      'AnalyticsStack',
      ecsCluster.ecsCluster,
      taskExecRoleStack.role,
      albStack.influxDBAlbDns
    );
    analyticsStack.addDependency(kinesisStack);
    analyticsStack.addDependency(influxStack);

    const backendStack = new BackendStack(
      this,
      'BackendStack',
      ecsCluster.ecsCluster,
      taskExecRoleStack.role,
      albStack.backendAlbListener,
      albStack.influxDBAlbDns
    );
    backendStack.addDependency(wsApigatewayStack);
    backendStack.addDependency(influxStack);
  }
}