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
import ExtractTextPlugin from 'extract-text-webpack-plugin';
import HtmlWebpackPlugin from 'html-webpack-plugin';
import ProgressBarPlugin from 'progress-bar-webpack-plugin';

import eslintFormatter from 'eslint-friendly-formatter';

const basedir = path.resolve('./');
const contentBase = path.resolve('public');
const node_modules = path.resolve('node_modules');

const app = path.resolve('app');
const IgniteModules = process.env.IGNITE_MODULES ? path.join(process.env.IGNITE_MODULES, 'frontend') : path.resolve('ignite_modules');

export default {
    node: {
        fs: 'empty'
    },
    // Entry points.
    entry: {
        app: path.join(app, 'app.js'),
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
            views: path.join(basedir, 'views'),
            Controllers: path.join(basedir, 'controllers'),
            IgniteModules
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
                loader: `pug-html?basedir=${basedir}`
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
                exclude: [node_modules],
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
                test: /\.(js)$/,
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
                exclude: [contentBase, IgniteModules],
                loader: 'file?name=assets/fonts/[name].[ext]'
            },
            {
                test: /.*\.svg$/,
                include: [contentBase, IgniteModules],
                use: ['svg-sprite-loader']
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
            template: './views/index.pug'
        }),
        new ExtractTextPlugin({filename: 'assets/css/[name].[hash].css', allChunks: true}),
        new CopyWebpackPlugin([
            { context: 'public', from: '**/*.png' },
            { context: 'public', from: '**/*.svg' },
            { context: 'public', from: '**/*.ico' },
            // Ignite modules.
            { context: IgniteModules, from: '**/*.png', force: true },
            { context: IgniteModules, from: '**/*.svg', force: true },
            { context: IgniteModules, from: '**/*.ico', force: true }
        ]),
        new ProgressBarPlugin()
    ]
};
