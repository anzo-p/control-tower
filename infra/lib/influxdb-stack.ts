import * as cdk from "aws-cdk-lib";
import * as ecr from "aws-cdk-lib/aws-ecr";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as efs from "aws-cdk-lib/aws-efs";
import * as elbv2 from "aws-cdk-lib/aws-elasticloadbalancingv2";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

export class InfluxDBStack extends cdk.NestedStack {
  constructor(
    scope: Construct,
    id: string,
    vpc: ec2.Vpc,
    ecsCluster: ecs.Cluster,
    influxDBAdminAlbListener: elbv2.ApplicationListener,
    executionRole: iam.Role,
    props?: cdk.StackProps
  ) {
    super(scope, id, props);

    const influxContainerRepository = "control-tower-influxdb";

    /*    
    // this would be the way to create and grant access to an efs if you create it now
    // for databases not advisable though, as data must retain, ie. we expect much data to exists already

    const influxDBFileSystem = new efs.FileSystem(
      this,
      "ControlTowerInfluxFileSystem",
      {
        vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      }
    );

    new efs.AccessPoint(this, "CTEfsAccessPoint", {
      fileSystem: influxDBFileSystem,
      path: "/var/lib/influxdb2",
      posixUser: {
        uid: "1000",
        gid: "1000",
      },
    });

    // here efs commands its sg to allow access, for existing efs you must allow from the sg directly (down below)
    influxDBFileSystem.connections.allowFrom(
      influxdbService,
      ec2.Port.tcp(2049),
      "Allow access to EFS on port 2049"
    );
    */

    const influxFileSystemSecurityGroup = new ec2.SecurityGroup(
      this,
      "ControlTowerInfluxDBEFSSecurityGroup",
      {
        vpc,
        allowAllOutbound: true,
      }
    );

    const influxDBFileSystem = efs.FileSystem.fromFileSystemAttributes(
      this,
      "ControlTowerInfluxFileSystem",
      {
        fileSystemId: "fs-0210b76a68f6092f8",
        securityGroup: ec2.SecurityGroup.fromSecurityGroupId(
          this,
          "ControlTowerInfluxFileSystemSecurityGroup",
          "sg-0f4c0d0a3f9f4f9d7"
        ),
      }
    );

    vpc.availabilityZones.forEach((_, index) => {
      new efs.CfnMountTarget(this, `EfsMountTarget${index}`, {
        fileSystemId: influxDBFileSystem.fileSystemId,
        subnetId: vpc.isolatedSubnets[index].subnetId,
        securityGroups: [influxFileSystemSecurityGroup.securityGroupId],
      });
    });

    const taskDefinition = new ecs.FargateTaskDefinition(
      this,
      "InfluxDBTaskDefinition",
      {
        family: "InfluxDBTaskDefinition",
        executionRole,
        runtimePlatform: {
          operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
          cpuArchitecture: ecs.CpuArchitecture.ARM64,
        },
        memoryLimitMiB: 1024,
        cpu: 512,
        volumes: [
          {
            name: "ControlTowerInfluxDBDataVolume",
            efsVolumeConfiguration: {
              fileSystemId: "fs-0210b76a68f6092f8",
              authorizationConfig: {
                iam: "ENABLED",
              },
              transitEncryption: "ENABLED",
            },
          },
        ],
      }
    );

    influxDBFileSystem.grant(
      taskDefinition.taskRole,
      "elasticfilesystem:ClientRootAccess",
      "elasticfilesystem:ClientMount",
      "elasticfilesystem:ClientWrite"
    );

    const ecrRepository = ecr.Repository.fromRepositoryName(
      this,
      "ECRRepository",
      influxContainerRepository
    );

    const influxDBContainer = taskDefinition.addContainer("InfluxDBContainer", {
      image: ecs.ContainerImage.fromEcrRepository(ecrRepository, "2.0"),
      portMappings: [{ protocol: ecs.Protocol.TCP, containerPort: 8086 }],
      memoryLimitMiB: 1024,
      cpu: 512,
      environment: {
        DOCKER_INFLUXDB_INIT_MODE: `${process.env.INFLUXDB_INIT_MODE}`,
        DOCKER_INFLUXDB_INIT_USERNAME: `${process.env.INFLUXDB_INIT_USERNAME}`,
        DOCKER_INFLUXDB_INIT_PASSWORD: `${process.env.INFLUXDB_INIT_PASSWORD}`,
        DOCKER_INFLUXDB_INIT_ORG: `${process.env.INFLUXDB_INIT_ORG}`,
        DOCKER_INFLUXDB_INIT_BUCKET: `${process.env.INFLUXDB_INIT_BUCKET}`,
        DOCKER_INFLUXDB_INIT_RETENTION: `${process.env.INFLUXDB_INIT_RETENTION}`,
        DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: `${process.env.INFLUXDB_INIT_ADMIN_TOKEN}`,
      },
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: "influxdb" }),
    });

    influxDBContainer.addMountPoints({
      containerPath: "/var/lib/influxdb2",
      readOnly: false,
      sourceVolume: "ControlTowerInfluxDBDataVolume",
    });

    const influxdbService = new ecs.FargateService(this, "InfluxDBEcsService", {
      cluster: ecsCluster,
      taskDefinition,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      desiredCount: 1,
      assignPublicIp: true,
    });

    influxdbService.registerLoadBalancerTargets({
      containerName: "InfluxDBContainer",
      containerPort: 8086,
      newTargetGroupId: "InfluxDBTargetGroup",
      listener: ecs.ListenerConfig.applicationListener(
        influxDBAdminAlbListener,
        {
          protocol: elbv2.ApplicationProtocol.HTTP,
          healthCheck: {
            path: "/health",
            interval: cdk.Duration.seconds(30),
            timeout: cdk.Duration.seconds(15),
            healthyThresholdCount: 3,
            unhealthyThresholdCount: 3,
            healthyHttpCodes: "200",
          },
        }
      ),
    });

    influxFileSystemSecurityGroup.connections.allowFrom(
      influxdbService,
      ec2.Port.tcp(2049),
      "Allow access to EFS on port 2049"
    );
  }
}
