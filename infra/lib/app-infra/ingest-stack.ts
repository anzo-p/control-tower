import * as cdk from 'aws-cdk-lib';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export class IngestStack extends cdk.NestedStack {
  constructor(
    scope: Construct,
    id: string,
    ecsCluster: ecs.Cluster,
    executionRole: iam.Role,
    writeKinesisUpstreamPerms: iam.PolicyStatement,
    props?: cdk.StackProps
  ) {
    super(scope, id, props);

    const secirutyGroup = new ec2.SecurityGroup(this, 'IngestSecurityGroup', {
      vpc: ecsCluster.vpc,
      allowAllOutbound: true
    });

    const taskRole = new iam.Role(this, 'TaskRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com')
    });

    taskRole.addToPolicy(writeKinesisUpstreamPerms);

    const taskDefinition = new ecs.FargateTaskDefinition(
      this,
      'IngestTaskDefinition',
      {
        family: 'IngestTaskDefinition',
        executionRole,
        taskRole,
        runtimePlatform: {
          operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
          cpuArchitecture: ecs.CpuArchitecture.X86_64
        },
        memoryLimitMiB: 512,
        cpu: 256
      }
    );

    const ecrRepository = ecr.Repository.fromRepositoryName(
      this,
      'EcrRepository',
      'stream-lines-ingest'
    );

    taskDefinition.addContainer('IngestContainer', {
      image: ecs.ContainerImage.fromEcrRepository(ecrRepository, 'latest'),
      memoryLimitMiB: 512,
      cpu: 256,
      environment: {
        ALPACA_API_KEY: `${process.env.INGEST_ALPACA_API_KEY}`,
        ALPACA_API_SECRET: `${process.env.INGEST_ALPACA_API_SECRET}`,
        KINESIS_UPSTREAM_NAME: `${process.env.KINESIS_MARKET_DATA_UPSTREAM}`,
        MAX_WS_READS_PER_SEC: `${process.env.INGEST_MAX_WS_READS_PER_SEC}`
      },
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'ingest' })
    });

    new ecs.FargateService(this, 'IngestEcsService', {
      cluster: ecsCluster,
      taskDefinition,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      securityGroups: [secirutyGroup],
      desiredCount: 1,
      assignPublicIp: true
    });
  }
}
