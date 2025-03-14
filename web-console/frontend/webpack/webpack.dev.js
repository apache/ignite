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

const merge = require('webpack-merge');

const path = require('path');

const commonCfg = require('./webpack.common');
const {devProdScss} = require('./styles');

const MiniCssExtractPlugin = require('mini-css-extract-plugin');

const backendUrl = process.env.BACKEND_URL || 'http://localhost:3000';
const webpackDevServerHost = process.env.HOST || '0.0.0.0';
const webpackDevServerPort = process.env.PORT || 9000;

console.log(`Backend url: ${backendUrl}`);

module.exports = merge(commonCfg, {
    mode: 'development',
    devtool: 'source-map',
    watch: true,
    module: {
        exprContextCritical: false,
        rules: [
            {
                test: /\.css$/,
                exclude: /\.url/,
                use: ['style-loader', 'css-loader']
            },
            ...devProdScss(true),
            {
                test: /\.html$/,
                use: 'file-loader'
            },
            {
                test: /\.(ts)$/,
                use: ['angular2-template-loader?keepUrl=true']
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
        contentBase: [path.resolve('build'),path.resolve('public')],
        inline: true,
        proxy: {
            '/browsers': {
                target: backendUrl,
                ws: true,
                secure: false
            },
            '/agents': {
                target: backendUrl,
                ws: true,
                secure: false
            },
            '/api/*': {
                target: backendUrl,
                secure: false
            }
        },
        watchOptions: {
            aggregateTimeout: 1000,
            poll: 2000
        },
        stats: 'errors-only',
        host: webpackDevServerHost,
        port: webpackDevServerPort
    }
});

// Prevents Webpack crashes on WS connection errors
process.addListener('uncaughtException', (e) => {
    console.warn(e);
});
