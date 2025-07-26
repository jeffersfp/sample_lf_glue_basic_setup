module.exports = {
    /* Jest runtime -------------------------------------------------------- */
    testEnvironment: 'node',

    /* Test discovery ------------------------------------------------------ */
    roots: [
        '<rootDir>/test',
    ],
    testMatch: [
        '**/*.test.ts',
    ],

    /* TypeScript transform ------------------------------------------------ */
    transform: {
        '^.+\\.tsx?$': 'ts-jest',
    },

    /* Coverage reporting -------------------------------------------------- */
    // Always collect coverage when “npm run test” is executed.
    collectCoverage: true,
    // Put the HTML report (plus a text summary) under build/coverage.
    coverageDirectory: 'build/coverage',
    coverageReporters: [
        'html',
        'text-summary',
    ],
};