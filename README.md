# AWS Lake Formation & Glue Basic Setup

A CDK sample repository demonstrating the fundamental building blocks for creating a data lake on AWS using Lake Formation and Glue.

## Overview

This project provides a complete CDK implementation of an AWS data lake with:
- **AWS Lake Formation** for data governance and access control
- **AWS Glue** for data cataloging with encrypted metadata
- **S3 buckets** with KMS encryption for data storage
- **Athena** workgroup for SQL queries
- Comprehensive security configurations

## Prerequisites

- AWS CLI configured with appropriate credentials
- Node.js and npm installed
- AWS CDK CLI installed (`npm install -g aws-cdk`)

## Quick Start

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Run tests
npm test

# Deploy to AWS
npm run deploy
```

## Project Structure

```
├── lib/
│   └── sample_lf_glue_basic_setup-stack.ts  # Main CDK stack
├── test/
│   └── sample_lf_glue_basic_setup.test.ts   # Comprehensive unit tests
├── bin/
│   └── sample_lf_glue_basic_setup.ts        # CDK app entry point
└── package.json
```

## Key Features

1. **Encrypted Storage**: All S3 buckets use KMS encryption with key rotation
2. **Data Governance**: Lake Formation controls access to databases and tables
3. **Secure Catalog**: Glue catalog encrypted with dedicated KMS key
4. **Access Control**: IAM user-based permissions for data access
5. **Query Isolation**: Dedicated Athena workgroup with result encryption

## Security

- All buckets enforce SSL and block public access
- KMS keys have automatic rotation enabled
- Lake Formation service-linked role manages data access
- Minimum required permissions granted