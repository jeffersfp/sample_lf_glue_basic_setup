/* eslint-disable @typescript-eslint/no-explicit-any */
import * as cdk from 'aws-cdk-lib';
import {
    Template, Match,
} from 'aws-cdk-lib/assertions';
import {
    SampleLfGlueBasicSetupStack,
} from '../lib/sample_lf_glue_basic_setup-stack';

describe('SampleLfGlueBasicSetupStack', () => {
    let app: cdk.App;
    let stack: SampleLfGlueBasicSetupStack;
    let template: Template;

    beforeEach(() => {
        app = new cdk.App();
        stack = new SampleLfGlueBasicSetupStack(app, 'TestStack');
        template = Template.fromStack(stack);
    });

    describe('KMS Keys', () => {
        test('creates three KMS keys with rotation enabled', () => {
            template.resourceCountIs('AWS::KMS::Key', 3);

            template.allResourcesProperties('AWS::KMS::Key', {
                EnableKeyRotation: true,
            });
        });
    });

    describe('S3 Buckets', () => {
        test('creates logging bucket with correct configuration', () => {
            template.hasResourceProperties('AWS::S3::Bucket', {
                AccessControl: 'LogDeliveryWrite',
                BucketEncryption: {
                    ServerSideEncryptionConfiguration: [
                        {
                            ServerSideEncryptionByDefault: {
                                SSEAlgorithm: 'AES256',
                            },
                        },
                    ],
                },
                PublicAccessBlockConfiguration: {
                    BlockPublicAcls: true,
                    BlockPublicPolicy: true,
                    IgnorePublicAcls: true,
                    RestrictPublicBuckets: true,
                },
                OwnershipControls: {
                    Rules: [
                        {
                            ObjectOwnership: 'ObjectWriter',
                        },
                    ],
                },
            });
        });

        test('creates data lake bucket with KMS encryption', () => {
            template.hasResourceProperties('AWS::S3::Bucket', {
                BucketEncryption: {
                    ServerSideEncryptionConfiguration: [
                        {
                            BucketKeyEnabled: true,
                            ServerSideEncryptionByDefault: {
                                SSEAlgorithm: 'aws:kms',
                                KMSMasterKeyID: Match.anyValue(),
                            },
                        },
                    ],
                },
                LoggingConfiguration: {
                    DestinationBucketName: Match.anyValue(),
                },
                OwnershipControls: {
                    Rules: [
                        {
                            ObjectOwnership: 'BucketOwnerEnforced',
                        },
                    ],
                },
            });
        });

        test('creates athena results bucket with KMS encryption', () => {
            const athenaStackBuckets = template.findResources('AWS::S3::Bucket');
            const athenaResultsBucket = Object.values(athenaStackBuckets).find(
                (resource) => {
                    const props = (resource as any).Properties;
                    return props.LoggingConfiguration?.LogFilePrefix &&
                           typeof props.LoggingConfiguration.LogFilePrefix === 'object' &&
                           JSON.stringify(props.LoggingConfiguration.LogFilePrefix).includes('athena-results-bucket-');
                },
            ) as any;
            expect(athenaResultsBucket).toBeDefined();
            expect(athenaResultsBucket?.Properties.BucketEncryption).toMatchObject({
                ServerSideEncryptionConfiguration: [
                    {
                        BucketKeyEnabled: true,
                        ServerSideEncryptionByDefault: {
                            SSEAlgorithm: 'aws:kms',
                        },
                    },
                ],
            });
        });
    });

    describe('Glue Data Catalog', () => {
        test('configures catalog encryption with KMS', () => {
            template.hasResourceProperties('AWS::Glue::DataCatalogEncryptionSettings', {
                CatalogId: Match.anyValue(),
                DataCatalogEncryptionSettings: {
                    EncryptionAtRest: {
                        CatalogEncryptionMode: 'SSE-KMS',
                        SseAwsKmsKeyId: Match.anyValue(),
                    },
                },
            });
        });

        test('creates Glue database', () => {
            template.hasResourceProperties('AWS::Glue::Database', {
                DatabaseInput: {
                    Name: 'sample_database',
                    Description: 'This is the description.',
                },
            });
        });

        test('creates Glue table with correct schema', () => {
            template.hasResourceProperties('AWS::Glue::Table', {
                TableInput: {
                    Name: 'sample_table',
                    Description: 'This is the table description',
                    StorageDescriptor: {
                        Columns: [
                            {
                                Name: 'number',
                                Type: 'int',
                                Comment: 'An integer.',
                            },
                        ],
                        InputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        OutputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        SerdeInfo: {
                            SerializationLibrary: 'org.openx.data.jsonserde.JsonSerDe',
                        },
                    },
                },
            });
        });
    });

    describe('Lake Formation', () => {
        test('configures data lake settings with admins', () => {
            template.hasResourceProperties('AWS::LakeFormation::DataLakeSettings', {
                Admins: Match.arrayWith([
                    Match.objectLike({
                        DataLakePrincipalIdentifier: Match.anyValue(),
                    }),
                ]),
                Parameters: {
                    CROSS_ACCOUNT_VERSION: 4,
                },
                MutationType: 'REPLACE',
            });
        });

        test('registers S3 location with Lake Formation', () => {
            template.hasResourceProperties('AWS::LakeFormation::Resource', {
                ResourceArn: Match.anyValue(),
                UseServiceLinkedRole: true,
                HybridAccessEnabled: true,
                RoleArn: Match.anyValue(),
            });
        });

        test('grants database permissions', () => {
            template.hasResourceProperties('AWS::LakeFormation::Permissions', {
                Permissions: [
                    'DESCRIBE',
                ],
                Resource: {
                    DatabaseResource: Match.objectLike({
                        CatalogId: Match.anyValue(),
                    }),
                },
                DataLakePrincipal: {
                    DataLakePrincipalIdentifier: Match.anyValue(),
                },
            });
        });

        test('grants table permissions', () => {
            template.hasResourceProperties('AWS::LakeFormation::Permissions', {
                Permissions: [
                    'DESCRIBE',
                    'SELECT',
                ],
                Resource: {
                    TableResource: Match.objectLike({
                        CatalogId: Match.anyValue(),
                    }),
                },
                DataLakePrincipal: {
                    DataLakePrincipalIdentifier: Match.anyValue(),
                },
            });
        });
    });

    describe('Athena', () => {
        test('creates Athena workgroup', () => {
            template.hasResourceProperties('AWS::Athena::WorkGroup', {
                Name: 'ReadOnly',
                WorkGroupConfiguration: {
                    PublishCloudWatchMetricsEnabled: true,
                    ResultConfiguration: {
                        OutputLocation: Match.anyValue(),
                    },
                },
            });
        });
    });

    describe('S3 Bucket Policies', () => {
        test('grants bucket access to Lake Formation service role', () => {
            template.hasResourceProperties('AWS::S3::BucketPolicy', {
                PolicyDocument: {
                    Statement: Match.arrayWith([
                        Match.objectLike({
                            Effect: 'Allow',
                            Principal: {
                                AWS: Match.anyValue(),
                            },
                            Action: Match.arrayWith([
                                's3:GetObject*',
                                's3:GetBucket*',
                                's3:List*',
                            ]),
                        }),
                    ]),
                },
            });
        });
    });
});