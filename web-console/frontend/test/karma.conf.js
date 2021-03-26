/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
