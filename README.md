# AWS Lake Formation & Glue Basic Setup

A CDK sample repository demonstrating the fundamental building blocks for creating a data lake on AWS using Lake Formation and Glue.

## Overview

This project provides a complete CDK implementation of an AWS data lake with:
- **AWS Lake Formation** for data governance and access control
- **AWS Glue** for data cataloging
- **Athena** workgroup for SQL queries

## Prerequisites

- AWS CLI configured with appropriate credentials
- Node.js and npm installed
- AWS CDK CLI installed (`npm install -g aws-cdk`)

## Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Update the `.env` file with your specific values:
```bash
# Lake Formation Admin Role ARN
LF_ADMIN_ROLE_ARN=arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_LAKEFORMATION_ADMIN_ROLE
```

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
├── .env.example                              # Environment variables template
└── package.json
```

## Key Features

1. **Data Governance**: Lake Formation controls access to databases and tables
2. **Access Control**: IAM user-based permissions for data access
3. **Query Isolation**: Dedicated Athena workgroup with result encryption and enhanced security
4. **Environment Configuration**: Flexible configuration through environment variables
