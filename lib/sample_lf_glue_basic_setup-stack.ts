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
    BucketDeployment, Source,
} from "aws-cdk-lib/aws-s3-deployment";
import {
    CfnDataCatalogEncryptionSettings, CfnTable,
} from "aws-cdk-lib/aws-glue";
import {
    ArnPrincipal, CfnRole,
} from "aws-cdk-lib/aws-iam";
import {
    CfnDataLakeSettings, CfnPermissions, CfnResource,
} from "aws-cdk-lib/aws-lakeformation";
import {
    Database,
} from "@aws-cdk/aws-glue-alpha";
import {
    CfnWorkGroup,
} from "aws-cdk-lib/aws-athena";

import 'dotenv/config';

export class SampleLfGlueBasicSetupStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);


        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /// IAM Roles and Principals

        const lakeFormationAdminRoleArn = process.env.LF_ADMIN_ROLE_ARN || '';
        if (!lakeFormationAdminRoleArn) {
            throw new Error('LF_ADMIN_ROLE_ARN environment variable is not set');
        }

        const lfServiceRoleArn = `arn:${this.partition}:iam::${this.account}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess`;

        // Create Lake Formation Service Role with necessary permissions.
        // Must use the Low-level CfnRole construct as there is no higher-level construct
        // for adding the sts:SetContext action to the assume role policy, which is required
        // for trusted identity propagation.
        // See: https://docs.aws.amazon.com/en_us/singlesignon/latest/userguide/tip-tutorial-lf.html
        const lakeFormationServiceRole = new CfnRole(this, 'LakeFormationServiceRole', {
            assumeRolePolicyDocument: {
                Version: '2012-10-17',
                Statement: [
                    {
                        Effect: 'Allow',
                        Principal: {
                            Service: 'lakeformation.amazonaws.com',
                        },
                        Action: [
                            'sts:AssumeRole',
                            'sts:SetContext',
                        ],
                    },
                ],
            },
            roleName: `LakeFormationServiceRole-${this.account}`,
            managedPolicyArns: [
                'arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin',
            ],
            policies: [
                {
                    policyName: 'S3DataAccess',
                    policyDocument: {
                        Version: '2012-10-17',
                        Statement: [
                            {
                                Sid: 'S3ReadWriteAccess',
                                Effect: 'Allow',
                                Action: [
                                    's3:GetObject',
                                    's3:GetObjectVersion',
                                    's3:PutObject',
                                    's3:DeleteObject',
                                    's3:ListAllMyBuckets',
                                    's3:ListBucket',
                                    's3:GetBucketLocation',
                                ],
                                Resource: '*',
                            },
                            {
                                Sid: 'LakeFormationPermissions',
                                Effect: 'Allow',
                                Action: [
                                    'lakeformation:GetDataAccess',
                                    'lakeformation:GrantPermissions',
                                    'lakeformation:RevokePermissions',
                                    'lakeformation:BatchGrantPermissions',
                                    'lakeformation:BatchRevokePermissions',
                                    'lakeformation:ListPermissions',
                                ],
                                Resource: '*',
                            },
                        ],
                    },
                },
            ],
        });

        const lfAdmins = [
            new ArnPrincipal(lakeFormationAdminRoleArn),
            new ArnPrincipal(lakeFormationServiceRole.attrArn),
            new ArnPrincipal(Fn.sub((this.synthesizer as DefaultStackSynthesizer).cloudFormationExecutionRoleArn)),
        ];
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /// Buckets

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
            encryption: BucketEncryption.S3_MANAGED,
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
            enforceSSL: true,
            objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
            serverAccessLogsBucket: loggingBucket,
            serverAccessLogsPrefix: `data-lake-bucket-${this.account}/`,
        });

        const athenaResultsBucket = new Bucket(this, 'AthenaResultsBucket', {
            bucketName: `athena-results-bucket-${this.account}`,
            encryption: BucketEncryption.S3_MANAGED,
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
            enforceSSL: true,
            objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
            serverAccessLogsBucket: loggingBucket,
            serverAccessLogsPrefix: `athena-results-bucket-${this.account}/`,
        });

        athenaResultsBucket.grantReadWrite(new ArnPrincipal(lfServiceRoleArn));
        dataLakeBucket.grantReadWrite(new ArnPrincipal(lfServiceRoleArn));
        athenaResultsBucket.grantReadWrite(new ArnPrincipal(lakeFormationServiceRole.attrArn));
        dataLakeBucket.grantReadWrite(new ArnPrincipal(lakeFormationServiceRole.attrArn));


        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /// Catalog settings

        new CfnDataCatalogEncryptionSettings(this, 'CatalogEncryptionSettings', {
            catalogId: this.account,
            dataCatalogEncryptionSettings: {
                encryptionAtRest: {
                    catalogEncryptionMode: 'SSE-S3',
                },
            },
        });

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
            useServiceLinkedRole: false,
            hybridAccessEnabled: true,
            roleArn: lakeFormationServiceRole.attrArn,
        });

        // new CfnResource(this, 'DataLakeRegisteredLocation', {
        //     resourceArn: `${dataLakeBucket.bucketArn}/`,
        //     useServiceLinkedRole: true,
        //     hybridAccessEnabled: true,
        //     roleArn: lfServiceRoleArn,
        // });


        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /// Database

        const databaseName = 'sales'

        const glueDatabase = new Database(this, 'SalesDatabase', {
            databaseName: databaseName,
            description: 'Sales database containing customer and order information.',
            locationUri: `s3://${dataLakeBucket.bucketName}/${databaseName}/`,
        });


        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /// Tables

        /* 
         * Alternative S3Table approach (Higher-level construct from @aws-cdk/aws-glue-alpha):
         * 
         * Advantages:
         * - Simplified API with intuitive, object-oriented interface
         * - Built-in defaults and automatic S3 integration
         * - Better TypeScript support with strongly-typed properties
         * - Less boilerplate code for common use cases
         * - Seamless integration with other CDK constructs
         * 
         * Disadvantages:
         * - Alpha package - API might change in future versions
         * - Less control over advanced Glue table features
         * - Requires separate @aws-cdk/aws-glue-alpha dependency
         * 
         * // Customers table
         * const customersTable = new S3Table(this, 'CustomersTable', {
         *     tableName: 'customers',
         *     description: 'Customer information table',
         *     columns: [
         *         {
         *             name: 'id',
         *             type: Schema.BIG_INT,
         *             comment: 'Customer ID',
         *         },
         *         {
         *             name: 'name',
         *             type: Schema.STRING,
         *             comment: 'Customer name',
         *         },
         *         {
         *             name: 'email',
         *             type: Schema.STRING,
         *             comment: 'Customer email address',
         *         },
         *     ],
         *     dataFormat: DataFormat.CSV,
         *     database: glueDatabase,
         *     bucket: dataLakeBucket,
         *     s3Prefix: `${glueDatabase.databaseName}/customers/`,
         * });
         * 
         * // Orders table
         * const ordersTable = new S3Table(this, 'OrdersTable', {
         *     tableName: 'orders',
         *     description: 'Customer orders table',
         *     columns: [
         *         {
         *             name: 'id',
         *             type: Schema.BIG_INT,
         *             comment: 'Order ID',
         *         },
         *         {
         *             name: 'customer_id',
         *             type: Schema.BIG_INT,
         *             comment: 'Customer ID (foreign key)',
         *         },
         *         {
         *             name: 'amount',
         *             type: Schema.FLOAT,
         *             comment: 'Order amount',
         *         },
         *     ],
         *     dataFormat: DataFormat.CSV,
         *     database: glueDatabase,
         *     bucket: dataLakeBucket,
         *     s3Prefix: `${glueDatabase.databaseName}/orders/`,
         * });
         */

        // CfnTable approach (Lower-level CloudFormation construct):
        // Provides complete control over all Glue table features
        // Part of stable CDK library, but requires more verbose configuration

        // Customers table
        const customersTable = new CfnTable(this, 'CustomersTable', {
            catalogId: this.account,
            databaseName: glueDatabase.databaseName,
            tableInput: {
                name: 'customers',
                description: 'Customer information table',
                tableType: 'EXTERNAL_TABLE',
                storageDescriptor: {
                    columns: [
                        {
                            name: 'id',
                            type: 'bigint',
                            comment: 'Customer ID',
                        },
                        {
                            name: 'name',
                            type: 'string',
                            comment: 'Customer name',
                        },
                        {
                            name: 'email',
                            type: 'string',
                            comment: 'Customer email address',
                        },
                    ],
                    location: `s3://${dataLakeBucket.bucketName}/${glueDatabase.databaseName}/customers/`,
                    inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    serdeInfo: {
                        serializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        parameters: {
                            'separatorChar': ',',
                            'skip.header.line.count': '1',
                        },
                    },
                },
            },
        });

        // Orders table
        const ordersTable = new CfnTable(this, 'OrdersTable', {
            catalogId: this.account,
            databaseName: glueDatabase.databaseName,
            tableInput: {
                name: 'orders',
                description: 'Customer orders table',
                tableType: 'EXTERNAL_TABLE',
                storageDescriptor: {
                    columns: [
                        {
                            name: 'id',
                            type: 'bigint',
                            comment: 'Order ID',
                        },
                        {
                            name: 'customer_id',
                            type: 'bigint',
                            comment: 'Customer ID (foreign key)',
                        },
                        {
                            name: 'amount',
                            type: 'float',
                            comment: 'Order amount',
                        },
                    ],
                    location: `s3://${dataLakeBucket.bucketName}/${glueDatabase.databaseName}/orders/`,
                    inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                    outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    serdeInfo: {
                        serializationLibrary: 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                        parameters: {
                            'separatorChar': ',',
                            'skip.header.line.count': '1',
                        },
                    },
                },
            },
        });


        ////////////////////////////////////////////////////
        ////////////////////////////////////////////////////
        ////////////////////////////////////////////////////
        ////////////////////////////////////////////////////
        // Permissions

        const databasePermission = new CfnPermissions(this, 'DatabasePermission', {
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
                dataLakePrincipalIdentifier: lakeFormationAdminRoleArn,
            },
        });
        databasePermission.addDependency(glueDatabase.node.defaultChild as CfnResource);

        const customersTablePermission = new CfnPermissions(this, 'CustomersTablePermission', {
            permissions: [
                'DESCRIBE',
                'SELECT',
            ],
            permissionsWithGrantOption: [

            ],
            resource: {
                tableResource: {
                    catalogId: this.account,
                    name: 'customers',
                    databaseName: glueDatabase.databaseName,
                },
            },
            dataLakePrincipal: {
                dataLakePrincipalIdentifier: lakeFormationAdminRoleArn,
            },
        });
        customersTablePermission.addDependency(customersTable);


        const ordersTablePermission = new CfnPermissions(this, 'OrdersTablePermission', {
            permissions: [
                'DESCRIBE',
                'SELECT',
            ],
            permissionsWithGrantOption: [

            ],
            resource: {
                tableResource: {
                    catalogId: this.account,
                    name: 'orders',
                    databaseName: glueDatabase.databaseName,
                },
            },
            dataLakePrincipal: {
                dataLakePrincipalIdentifier: lakeFormationAdminRoleArn,
            },
        });
        ordersTablePermission.addDependency(ordersTable);


        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /// Data Deployment

        new BucketDeployment(this, 'SampleDataDeployment', {
            sources: [
                Source.asset('lib/data'),
            ],
            destinationBucket: dataLakeBucket,
            destinationKeyPrefix: 'sales/',
        });


        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /////////////////////////////////////////////////////
        /// Athena WorkGroup

        // TODO: Make it work with Lake Formation and Identity Center
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
