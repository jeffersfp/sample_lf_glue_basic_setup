/**
 * ESLint flat-config for the CDK project (ESLint ≥ v9).
 *
 * Layers:
 * 1. Base JS rules + Node globals.
 * 2. TypeScript: parser + rules from @typescript-eslint.
 * 3. Tests: Jest plugin flat preset + combined Node/Jest globals.
 */

const js = require('@eslint/js');
const globalsDB = require('globals');
const tsPlugin = require('@typescript-eslint/eslint-plugin');
const tsParser = require('@typescript-eslint/parser');
const jestPlugin = require('eslint-plugin-jest');

/** @type {import('eslint').Linter.FlatConfig[]} */
module.exports = [
    // ── Global ignores ──────────────────────────────────────────────────────
    {
        ignores: [
            'node_modules/**',
            'cdk.out/**',
            '.cdk.staging/**',
            '**/*.d.ts',
            '**/*.js',  // Ignore all compiled JavaScript files
            'coverage/**',
            'dist/**',
            'build/**',
        ],
    },
    // ── Base ────────────────────────────────────────────────────────────────
    {
        ...js.configs.recommended,
        languageOptions: {
            ecmaVersion: 'latest',
            sourceType: 'module',
            globals: {
                ...globalsDB.node,
            },          // require, module, __dirname…
        },
        rules: {
            /* Style rules ------------------------------------------------------- */
            // Trailing commas in multiline structures.
            'comma-dangle': [
                'error',
                'always-multiline',
            ],
            // 4-space indentation everywhere (cases indented 1 level inside switch).
            'indent': [
                'error',
                4,
                {
                    SwitchCase: 1,
                },
            ],
            // Force multiline style for arrays & objects.
            'array-bracket-newline': [
                'error',
                'always',
            ],
            'array-element-newline': [
                'error',
                'always',
            ],
            'object-curly-newline': [
                'error',
                {
                    minProperties: 1,
                },
            ],
            'object-property-newline': [
                'error',
                {
                    allowAllPropertiesOnSameLine: false,
                },
            ],
        },
    },

    // ── TypeScript ──────────────────────────────────────────────────────────
    {
        files: [
            '**/*.ts',
            '**/*.tsx',
        ],
        languageOptions: {
            parser: tsParser,
            parserOptions: {
                project: './tsconfig.json',
                ecmaVersion: 'latest',
                sourceType: 'module',
            },
            globals: {
                ...globalsDB.node,
            },
        },
        plugins: {
            '@typescript-eslint': tsPlugin,
        },
        rules: {
            ...tsPlugin.configs.recommended.rules,
            /* Style rules ------------------------------------------------------- */
            'comma-dangle': [
                'error',
                'always-multiline',
            ],
            'indent': [
                'error',
                4,
                {
                    SwitchCase: 1,
                },
            ],
            'array-bracket-newline': [
                'error',
                'always',
            ],
            'array-element-newline': [
                'error',
                'always',
            ],
            'object-curly-newline': [
                'error',
                {
                    minProperties: 1,
                },
            ],
            'object-property-newline': [
                'error',
                {
                    allowAllPropertiesOnSameLine: false,
                },
            ],
        },
    },

    // ── Jest tests ──────────────────────────────────────────────────────────
    {
        files: [
            '**/*.test.ts',
            '**/*.test.tsx',
        ],
        /* Spread the plugin’s flat-config preset (defines plugins, rules, etc.) */
        ...jestPlugin.configs['flat/recommended'],
        /* Merge Node globals with Jest globals. */
        languageOptions: {
            globals: {
                ...globalsDB.node,
                ...globalsDB.jest,
            },
        },
        rules: {
            "jest/expect-expect": [
                "error",
                {
                    "assertFunctionNames": [

                        "template.*",
                    ],
                },
            ],
        },
    },
];