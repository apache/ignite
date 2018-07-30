/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import path from 'path';
import webpack from 'webpack';

import transformRuntime from 'babel-plugin-transform-runtime';
import presetEs2015 from 'babel-preset-es2015';
import presetStage1 from 'babel-preset-stage-1';

import CopyWebpackPlugin from 'copy-webpack-plugin';
import HtmlWebpackPlugin from 'html-webpack-plugin';
import ProgressBarPlugin from 'progress-bar-webpack-plugin';

import eslintFormatter from 'eslint-friendly-formatter';

const basedir = path.join(__dirname, '../');
const contentBase = path.join(basedir, 'public');
const node_modules = path.join(basedir, 'node_modules');
const app = path.join(basedir, 'app');

/** @type {webpack.Configuration} */
const config = {
    node: {
        fs: 'empty'
    },
    // Entry points.
    entry: {
        app: path.join(basedir, 'index.js'),
        browserUpdate: path.join(app, 'browserUpdate', 'index.js')
    },

    // Output system.
    output: {
        path: path.resolve('build'),
        filename: '[name].[chunkhash].js',
        publicPath: '/'
    },

    // Resolves modules.
    resolve: {
        modules: [node_modules],
        // A list of module source folders.
        alias: {
            app,
            images: path.join(basedir, 'public/images'),
            views: path.join(basedir, 'views')
        }
    },

    // Resolve loader use postfix.
    resolveLoader: {
        modules: [
            node_modules
        ],
        moduleExtensions: ['-loader']
    },

    module: {
        rules: [
            // Exclude tpl.pug files to import in bundle.
            {
                test: /^(?:(?!tpl\.pug$).)*\.pug$/, // TODO: check this regexp for correct.
                loader: 'pug-html',
                query: {
                    basedir
                }
            },

            // Render .tpl.pug files to assets folder.
            {
                test: /\.tpl\.pug$/,
                use: [
                    'file?exports=false&name=assets/templates/[name].[hash].html',
                    `pug-html?exports=false&basedir=${basedir}`
                ]
            },
            { test: /\.worker\.js$/, use: { loader: 'worker-loader' } },
            {
                test: /\.js$/,
                enforce: 'pre',
                exclude: [/node_modules/],
                use: [{
                    loader: 'eslint',
                    options: {
                        failOnWarning: false,
                        failOnError: false,
                        formatter: eslintFormatter,
                        context: process.cwd()
                    }
                }]
            },
            {
                test: /\.js$/,
                exclude: [node_modules],
                use: [{
                    loader: 'babel-loader',
                    options: {
                        cacheDirectory: true,
                        plugins: [
                            transformRuntime
                        ],
                        presets: [
                            presetEs2015,
                            presetStage1
                        ]
                    }
                }]
            },
            {
                test: /\.(ttf|eot|svg|woff(2)?)(\?v=[\d.]+)?(\?[a-z0-9#-]+)?$/,
                exclude: [contentBase],
                loader: 'file?name=assets/fonts/[name].[ext]'
            },
            {
                test: /^(?:(?!url\.svg$).)*\.svg$/,
                include: [contentBase],
                loader: 'svg-sprite-loader'
            },
            {
                test: /.*\.url\.svg$/,
                include: [contentBase],
                loader: 'file?name=assets/fonts/[name].[ext]'
            },
            {
                test: /\.(jpe?g|png|gif)$/i,
                loader: 'file?name=assets/images/[name].[hash].[ext]'
            },
            {
                test: require.resolve('jquery'),
                use: [
                    'expose-loader?$',
                    'expose-loader?jQuery'
                ]
            },
            {
                test: require.resolve('nvd3'),
                use: ['expose-loader?nv']
            }
        ]
    },

    optimization: {
        splitChunks: {
            chunks: 'all'
        }
    },

    // Load plugins.
    plugins: [
        new webpack.LoaderOptionsPlugin({
            options: {
                pug: {
                    basedir
                },
                eslint: {
                    configFile: path.join(basedir, '.eslintrc')
                },
                target: 'web'
            }
        }),
        new webpack.ProvidePlugin({
            $: 'jquery',
            'window.jQuery': 'jquery',
            _: 'lodash',
            nv: 'nvd3',
            io: 'socket.io-client'
        }),
        new webpack.optimize.AggressiveMergingPlugin({moveToParents: true}),
        new HtmlWebpackPlugin({
            template: path.join(basedir, './views/index.pug')
        }),
        new CopyWebpackPlugin([
            { context: 'public', from: '**/*.{png,svg,ico}' }
        ]),
        new ProgressBarPlugin()
    ]
};

export default config;
