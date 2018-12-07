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

const path = require('path');
const webpack = require('webpack');

const CopyWebpackPlugin = require('copy-webpack-plugin');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const ProgressBarPlugin = require('progress-bar-webpack-plugin');

const eslintFormatter = require('eslint-formatter-friendly');

const basedir = path.join(__dirname, '../');
const contentBase = path.join(basedir, 'public');
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
        // A list of module source folders.
        alias: {
            app,
            images: path.join(basedir, 'public/images'),
            views: path.join(basedir, 'views')
        },
        extensions: ['.wasm', '.mjs', '.js', '.ts', '.json']
    },

    module: {
        rules: [
            // Exclude tpl.pug files to import in bundle.
            {
                test: /^(?:(?!tpl\.pug$).)*\.pug$/, // TODO: check this regexp for correct.
                use: {
                    loader: 'pug-html-loader',
                    options: {
                        basedir
                    }
                }
            },

            // Render .tpl.pug files to assets folder.
            {
                test: /\.tpl\.pug$/,
                use: [
                    'file-loader?exports=false&name=assets/templates/[name].[hash].html',
                    `pug-html-loader?exports=false&basedir=${basedir}`
                ]
            },
            { test: /\.worker\.js$/, use: { loader: 'worker-loader' } },
            {
                test: /\.(js|ts)$/,
                enforce: 'pre',
                exclude: [/node_modules/],
                use: [{
                    loader: 'eslint-loader',
                    options: {
                        formatter: eslintFormatter,
                        context: process.cwd()
                    }
                }]
            },
            {
                test: /\.(js|ts)$/,
                exclude: /node_modules/,
                use: 'babel-loader'
            },
            {
                test: /\.(ttf|eot|svg|woff(2)?)(\?v=[\d.]+)?(\?[a-z0-9#-]+)?$/,
                exclude: [contentBase, /\.icon\.svg$/],
                use: 'file-loader?name=assets/fonts/[name].[ext]'
            },
            {
                test: /\.icon\.svg$/,
                use: {
                    loader: 'svg-sprite-loader',
                    options: {
                        symbolRegExp: /\w+(?=\.icon\.\w+$)/,
                        symbolId: '[0]'
                    }
                }
            },
            {
                test: /.*\.url\.svg$/,
                include: [contentBase],
                use: 'file-loader?name=assets/fonts/[name].[ext]'
            },
            {
                test: /\.(jpe?g|png|gif)$/i,
                use: 'file-loader?name=assets/images/[name].[hash].[ext]'
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
                use: 'expose-loader?nv'
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

module.exports = config;
