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

import {destDir, rootDir, srcDir} from '../../paths';

const backendPort = 3000;
const devServerPort = 9000;

export default () => {
    const plugins = [
        new webpack.HotModuleReplacementPlugin()
    ];

    return {
        entry: {
            app: [path.join(srcDir, 'app.js'), 'webpack/hot/only-dev-server']
        },
        output: {
            publicPath: `http://localhost:${devServerPort}/`
        },
        context: rootDir,
        debug: true,
        devtool: 'source-map',
        watch: true,
        devServer: {
            compress: true,
            historyApiFallback: true,
            contentBase: destDir,
            hot: true,
            inline: true,
            proxy: {
                '/socket.io': {
                    target: `http://localhost:${backendPort}`,
                    changeOrigin: true,
                    ws: true
                },
                '/agents': {
                    target: `http://localhost:${backendPort}`,
                    changeOrigin: true,
                    ws: true
                },
                '/api/v1/*': {
                    target: `http://localhost:${backendPort}`,
                    changeOrigin: true,
                    pathRewrite: {
                        '^/api/v1': ''
                    }
                }
            },
            watchOptions: {
                aggregateTimeout: 1000,
                poll: 2000
            },
            stats: {
                colors: true,
                chunks: false
            },
            port: devServerPort
        },
        plugins
    };
};
