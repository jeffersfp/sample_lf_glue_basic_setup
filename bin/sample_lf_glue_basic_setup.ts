#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {
    SampleLfGlueBasicSetupStack, 
} from '../lib/sample_lf_glue_basic_setup-stack';

const app = new cdk.App();
new SampleLfGlueBasicSetupStack(app, 'SampleLfGlueBasicSetupStack');