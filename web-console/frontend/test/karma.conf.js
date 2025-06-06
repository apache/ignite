

const path = require('path');

const testCfg = require('../webpack/webpack.test');

module.exports = (/** @type {import('karma').Config} */ config) => {
    config.set({
        // Base path that will be used to resolve all patterns (eg. files, exclude).
        basePath: path.resolve('./'),

        // Frameworks to use available frameworks: https://npmjs.org/browse/keyword/karma-adapter
        frameworks: ['mocha'],

        // List of files / patterns to load in the browser.
        files: [
            'node_modules/angular/angular.js',
            'node_modules/angular-mocks/angular-mocks.js',
            'test/zone-testing-mocha.ts',
            '+(app|app-angular)/**/*.spec.+(js|ts)',
            'test/**/*.test.js'
        ],

        plugins: [
            require('karma-chrome-launcher'),
            require('karma-teamcity-reporter'),
            require('karma-mocha-reporter'),
            require('karma-webpack'),
            require('karma-mocha')
        ],

        // Preprocess matching files before serving them to the browser
        // available preprocessors: https://npmjs.org/browse/keyword/karma-preprocessor.
        preprocessors: {
            '+(app|app-angular|test)/**/*.+(js|ts)': ['webpack']
        },

        webpack: testCfg,

        webpackMiddleware: {
            noInfo: true
        },

        // Test results reporter to use
        // possible values: 'dots', 'progress'
        // available reporters: https://npmjs.org/browse/keyword/karma-reporter.
        reporters: [process.env.TEST_REPORTER || 'mocha'],

        mochaReporter: {
            showDiff: true
        },

        // web server port
        port: 9876,

        // enable / disable colors in the output (reporters and logs)
        colors: true,

        // level of logging
        // possible values: config.LOG_DISABLE || config.LOG_ERROR || config.LOG_WARN || config.LOG_INFO || config.LOG_DEBUG
        logLevel: config.LOG_INFO,

        // enable / disable watching file and executing tests whenever any file changes
        autoWatch: true,

        // start these browsers
        // available browser launchers: https://npmjs.org/browse/keyword/karma-launcher
        browsers: ['ChromeHeadlessNoSandbox'],
        customLaunchers: {
            ChromeHeadlessNoSandbox: {
                base: 'ChromeHeadless',
                flags: ['--no-sandbox']
            },
            ChromeDebug: {
                base: 'Chrome',
                flags: [
                    '--start-maximized',
                    '--auto-open-devtools-for-tabs'
                ],
                debug: true
            }
        },

        // Continuous Integration mode
        // if true, Karma captures browsers, runs the tests and exits
        singleRun: true,

        // Concurrency level
        // how many browser should be started simultaneous
        concurrency: Infinity,

        client: {
            mocha: {
                ui: 'tdd'
            }
        }
    });
};
