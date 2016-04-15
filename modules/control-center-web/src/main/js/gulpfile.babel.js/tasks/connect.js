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

import gulp from 'gulp';
import connect from 'gulp-connect';
import proxy from 'http-proxy-middleware';

import { destDir } from '../paths';

// Task run static server to local development.
gulp.task('connect', () => {
    connect.server({
        port: 8090,
        root: [destDir],
        middleware() {
            return [
                proxy('/socket.io', {
                    target: 'http://localhost:3000',
                    changeOrigin: true,
                    ws: true
                }),
                proxy('/api/v1/', {
                    target: 'http://localhost:3000',
                    changeOrigin: true,
                    pathRewrite: {
                        '^/api/v1/': '/' // remove path
                    }
                })
            ];
        },
        fallback: `${destDir}/index.html`
    });
});
