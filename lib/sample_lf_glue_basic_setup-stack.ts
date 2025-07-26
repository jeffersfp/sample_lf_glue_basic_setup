import * as cdk from 'aws-cdk-lib';
import {
    DefaultStackSynthesizer, Fn, RemovalPolicy,
} from 'aws-cdk-lib';
import {
    Construct,
} from 'constructs';
import {
    BlockPublicAccess, Bucket, BucketEncryption, ObjectOwnership,
} from "aws-cdk-lib/aws-s3";
import {
    Key,
} from "aws-cdk-lib/aws-kms";
import {
    CfnDataCatalogEncryptionSettings,
} from "aws-cdk-lib/aws-glue";
import {
    ArnPrincipal,
} from "aws-cdk-lib/aws-iam";
import {
    CfnDataLakeSettings, CfnPermissions, CfnResource,
} from "aws-cdk-lib/aws-lakeformation";
import {
    Database, DataFormat, S3Table, Schema,
} from "@aws-cdk/aws-glue-alpha";
import {
    CfnWorkGroup,
} from "aws-cdk-lib/aws-athena";

export class SampleLfGlueBasicSetupStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Security

        const myUser = new ArnPrincipal(`arn:aws:iam::${this.account}:user/rodrigo`)

        const dataLakeBucketKmsKey = new Key(this, 'DataLakeBucketKmsKey', {
            enableKeyRotation: true,
            removalPolicy: RemovalPolicy.DESTROY,
        });

        const athenaResultsBucketKmsKey = new Key(this, 'AthenaResultsBucketKmsKey', {
            enableKeyRotation: true,
            removalPolicy: RemovalPolicy.DESTROY,
        });

        const catalogKmsKey = new Key(this, 'CatalogKmsKey', {
            enableKeyRotation: true,
            removalPolicy: RemovalPolicy.DESTROY,
        });

        const lfServiceRoleArn = `arn:${this.partition}:iam::${this.account}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess`;

        const lfAdmins = [
            myUser,
            new ArnPrincipal(Fn.sub((this.synthesizer as DefaultStackSynthesizer).cloudFormationExecutionRoleArn)),
        ];

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Buckets

        const loggingBucket = new Bucket(this, 'LoggingBucket', {
            bucketName: `logging-${this.account}`,
            encryption: BucketEncryption.S3_MANAGED,
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
            enforceSSL: true,
            objectOwnership: ObjectOwnership.OBJECT_WRITER,
        });

        const dataLakeBucket = new Bucket(this, 'DataLakeBucket', {
            bucketName: `data-lake-bucket-${this.account}`,
            encryptionKey: dataLakeBucketKmsKey,
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
            enforceSSL: true,
            objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
            bucketKeyEnabled: true,
            serverAccessLogsBucket: loggingBucket,
            serverAccessLogsPrefix: `data-lake-bucket-${this.account}/`,
        });

        const athenaResultsBucket = new Bucket(this, 'AthenaResultsBucket', {
            bucketName: `athena-results-bucket-${this.account}`,
            encryptionKey: athenaResultsBucketKmsKey,
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
            enforceSSL: true,
            objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
            bucketKeyEnabled: true,
            serverAccessLogsBucket: loggingBucket,
            serverAccessLogsPrefix: `athena-results-bucket-${this.account}/`,
        });

        athenaResultsBucket.grantReadWrite(new ArnPrincipal(lfServiceRoleArn));
        dataLakeBucket.grantReadWrite(new ArnPrincipal(lfServiceRoleArn));

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Catalog settings

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Catalog encryption

        new CfnDataCatalogEncryptionSettings(this, 'CatalogEncryptionSettings', {
            catalogId: this.account,
            dataCatalogEncryptionSettings: {
                encryptionAtRest: {
                    catalogEncryptionMode: 'SSE-KMS',
                    sseAwsKmsKeyId: catalogKmsKey.keyId,
                },
            },
        });

        lfAdmins.forEach(admin => {
            catalogKmsKey.grantEncryptDecrypt(admin);
        })

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Catalog settings

        new CfnDataLakeSettings(this, 'DataLakeSettings', {
            admins: lfAdmins.map(admin => ({
                dataLakePrincipalIdentifier: admin.arn,
            })),
            parameters: {
                CROSS_ACCOUNT_VERSION: 4,
            },
            mutationType: 'REPLACE',
            createDatabaseDefaultPermissions: [

            ],
            createTableDefaultPermissions: [

            ],
        });

        new CfnResource(this, 'DataLakeRegisteredLocation', {
            resourceArn: `${dataLakeBucket.bucketArn}/`,
            useServiceLinkedRole: true,
            hybridAccessEnabled: true,
            roleArn: lfServiceRoleArn,
        });

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Database

        const databaseName = 'sample_database'

        const glueDatabase = new Database(this, 'SampleDatabase', {
            databaseName: databaseName,
            description: 'This is the description.',
            locationUri: `s3://${dataLakeBucket.bucketName}/${databaseName}/`,
        });

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Table

        const tableName = 'sample_table';

        const glueTable = new S3Table(this, 'SampleTable', {
            tableName: tableName,
            description: 'This is the table description',
            columns: [
                {
                    name: 'number',
                    type: Schema.INTEGER,
                    comment: 'An integer.',
                },
            ],
            dataFormat: DataFormat.JSON,
            database: glueDatabase,
            bucket: dataLakeBucket,
            s3Prefix: `${glueDatabase.databaseName}/${tableName}/`,
        });

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Permissions

        new CfnPermissions(this, 'DatabasePermission', {
            permissions: [
                'DESCRIBE',
            ],
            permissionsWithGrantOption: [

            ],
            resource: {
                databaseResource: {
                    catalogId: this.account,
                    name: glueDatabase.databaseName,
                },
            },
            dataLakePrincipal: {
                dataLakePrincipalIdentifier: myUser.arn,
            },
        });

        new CfnPermissions(this, 'TablePermission', {
            permissions: [
                'DESCRIBE',
                'SELECT',
            ],
            permissionsWithGrantOption: [

            ],
            resource: {
                tableResource: {
                    catalogId: this.account,
                    name: glueTable.tableName,
                    databaseName: glueDatabase.databaseName,
                },
            },
            dataLakePrincipal: {
                dataLakePrincipalIdentifier: myUser.arn,
            },
        });

        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        /// /////////////////////////////////////////////////
        // Permissions

        new CfnWorkGroup(this, 'ReadOnlyWorkGroup', {
            name: 'ReadOnly',
            workGroupConfiguration: {
                publishCloudWatchMetricsEnabled: true,
                resultConfiguration: {
                    outputLocation: `s3://${athenaResultsBucket.bucketName}/ReadOnlyWorkGroup`,
                },
            },
        })
    }
}
