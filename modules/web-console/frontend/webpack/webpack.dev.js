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

const merge = require('webpack-merge');

const path = require('path');

const commonCfg = require('./webpack.common');

const MiniCssExtractPlugin = require('mini-css-extract-plugin');

const backendPort = process.env.BACKEND_PORT || 3000;
const devServerPort = process.env.PORT || 9000;
const devServerHost = process.env.HOST || '0.0.0.0';

module.exports = merge(commonCfg, {
    mode: 'development',
    devtool: 'source-map',
    watch: true,
    module: {
        exprContextCritical: false,
        rules: [
            {
                test: /\.css$/,
                use: ['style-loader', 'css-loader']
            },
            {
                test: /\.scss$/,
                use: [
                    MiniCssExtractPlugin.loader, // style-loader does not work with styles in IgniteModules
                    {
                        loader: 'css-loader',
                        options: {
                            sourceMap: true
                        }
                    },
                    {
                        loader: 'sass-loader',
                        options: {
                            sourceMap: true,
                            includePaths: [ path.join(__dirname, '../') ]
                        }
                    }
                ]
            }
        ]
    },
    plugins: [
        new MiniCssExtractPlugin({filename: 'assets/css/[name].css'})
    ],
    devServer: {
        compress: true,
        historyApiFallback: true,
        disableHostCheck: true,
        contentBase: path.resolve('build'),
        inline: true,
        proxy: {
            '/socket.io': {
                target: `http://localhost:${backendPort}`,
                ws: true
            },
            '/agents': {
                target: `http://localhost:${backendPort}`,
                ws: true
            },
            '/api/*': {
                target: `http://localhost:${backendPort}`
            }
        },
        watchOptions: {
            aggregateTimeout: 1000,
            poll: 2000
        },
        stats: 'errors-only',
        host: devServerHost,
        port: devServerPort
    }
});
