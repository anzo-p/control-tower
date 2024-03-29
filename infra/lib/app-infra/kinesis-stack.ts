import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import { KinesisEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

export class KinesisStreamsStack extends cdk.NestedStack {
  readonly writeUpstreamPerms: iam.PolicyStatement;
  readonly readUpstreamPerms: iam.PolicyStatement;
  readonly writeDownstreamPerms: iam.PolicyStatement;

  makeWriteAccessPolicy(streamName: string): iam.PolicyStatement {
    return new iam.PolicyStatement({
      actions: [
        'kinesis:PutRecord',
        'kinesis:PutRecords',
        'kinesis:DescribeStream'
      ],
      effect: iam.Effect.ALLOW,
      resources: [
        `arn:aws:kinesis:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT}:stream/${streamName}`
      ]
    });
  }

  makeReadAccessPolicy(streamName: string): iam.PolicyStatement {
    return new iam.PolicyStatement({
      actions: [
        'kinesis:DescribeStream',
        'kinesis:GetRecord',
        'kinesis:GetRecords',
        'kinesis:GetShardIterator',
        'kinesis:ListShards',
        'kinesis:SubscribeToShard'
      ],
      effect: iam.Effect.ALLOW,
      resources: [
        `arn:aws:kinesis:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT}:stream/${streamName}`
      ]
    });
  }

  constructor(
    scope: Construct,
    id: string,
    wsApiGatewayStageProdArn: string,
    wsApiGatewayConnectionsUrl: string,
    props?: cdk.StackProps
  ) {
    super(scope, id, props);

    new kinesis.Stream(this, 'MarketDataUpStream', {
      streamName: 'stream-lines-market-data-upstream',
      shardCount: 2,
      retentionPeriod: cdk.Duration.hours(24)
    });

    const resultsStream = new kinesis.Stream(this, 'ResultsDownStream', {
      streamName: 'stream-lines-results-downstream',
      shardCount: 2,
      retentionPeriod: cdk.Duration.hours(24)
    });

    const roleResultsStreamPusherLambda = new iam.Role(
      this,
      'KinesisStreamPusherRole',
      {
        assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AWSLambdaBasicExecutionRole'
          )
        ]
      }
    );

    roleResultsStreamPusherLambda.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'logs:CreateLogGroup',
          'logs:CreateLogStream',
          'logs:PutLogEvents'
        ],
        effect: iam.Effect.ALLOW,
        resources: ['arn:aws:logs:*:*:*']
      })
    );

    roleResultsStreamPusherLambda.addToPolicy(
      new iam.PolicyStatement({
        actions: ['dynamodb:Query', 'dynamodb:DeleteItem'],
        effect: iam.Effect.ALLOW,
        resources: [
          `arn:aws:dynamodb:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT}:table/${process.env.WS_CONNS_TABLE_NAME}`,
          `arn:aws:dynamodb:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT}:table/${process.env.WS_CONNS_TABLE_NAME}/index/${process.env.WS_CONNS_BY_SYMBOL_INDEX}`
        ]
      })
    );

    roleResultsStreamPusherLambda.addToPolicy(
      this.makeReadAccessPolicy(resultsStream.streamName)
    );

    roleResultsStreamPusherLambda.addToPolicy(
      new iam.PolicyStatement({
        actions: ['execute-api:ManageConnections'],
        resources: [wsApiGatewayStageProdArn]
      })
    );

    const bucketResultStreamPusherLambda = s3.Bucket.fromBucketName(
      this,
      'LambdaSourceBucket',
      `${process.env.S3_APP_BUCKET}`
    );

    const resultsStreamPusherLambda = new lambda.Function(
      this,
      'StreamPusherLambda',
      {
        functionName: 'KinesisResultsStreamPusher',
        runtime: lambda.Runtime.NODEJS_20_X,
        handler: 'index.handler',
        code: lambda.Code.fromBucket(
          bucketResultStreamPusherLambda,
          `${process.env.S3_KEY_RESULTS_PUSHER}`
        ),
        role: roleResultsStreamPusherLambda,
        environment: {
          API_GW_CONNECTIONS_URL: `${wsApiGatewayConnectionsUrl}`,
          WS_CONNS_TABLE_NAME: `${process.env.WS_CONNS_TABLE_NAME}`,
          WS_CONNS_BY_SYMBOL_INDEX: `${process.env.WS_CONNS_BY_SYMBOL_INDEX}`
        }
      }
    );

    resultsStreamPusherLambda.addEventSource(
      new KinesisEventSource(resultsStream, {
        startingPosition: lambda.StartingPosition.LATEST,
        batchSize: 50,
        maxBatchingWindow: cdk.Duration.seconds(15),
        retryAttempts: 3
      })
    );

    this.writeUpstreamPerms = this.makeWriteAccessPolicy(
      'stream-lines-market-data-upstream'
    );
    this.readUpstreamPerms = this.makeReadAccessPolicy(
      'stream-lines-market-data-upstream'
    );
    this.writeDownstreamPerms = this.makeWriteAccessPolicy(
      'stream-lines-results-downstream'
    );
  }
}
