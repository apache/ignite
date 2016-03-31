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
import jspm from 'jspm';
import util from 'gulp-util';
import sequence from 'gulp-sequence';
import htmlReplace from 'gulp-html-replace';

import { srcDir, destDir, igniteModulesTemp } from '../paths';

const paths = [
    './app/**/*.js',
    './app/**/*.jade',
    './app/data/*.json'
];

const options = {
    minify: true
};

gulp.task('bundle', ['eslint', 'bundle:ignite']);

// Package all external dependencies and ignite-console.
gulp.task('bundle:ignite', (cb) => {
    if (util.env.debug)
        delete options.minify;

    if (util.env.debug || util.env.sourcemaps)
        options.sourceMaps = true;

    if (util.env.debug)
        return sequence('bundle:ignite:vendors', 'bundle:ignite:app', cb);

    return sequence('bundle:ignite:app-min', 'bundle:ignite:app-min:replace', cb);
});

gulp.task('bundle:ignite:watch', () =>
    gulp.watch(paths, ['bundle:ignite:app'])
);

gulp.task('bundle:ignite:vendors', () => {
    const exclude = [
        `${srcDir}/**/*`,
        `${srcDir}/**/*!jade`,
        './controllers/**/*',
        './helpers/**/*',
        './public/**/*!css',
        `${igniteModulesTemp}/**/*`,
        `${igniteModulesTemp}/**/*!jade`
    ].map((item) => `[${item}]`).join(' - ');

    return jspm.bundle(`${srcDir}/index - ${exclude}`, `${destDir}/vendors.js`, options);
});

gulp.task('bundle:ignite:app', () =>
    jspm.bundle(`${srcDir}/index - ${destDir}/vendors`, `${destDir}/app.js`, options)
);

gulp.task('bundle:ignite:app-min', () =>
    jspm.bundleSFX(`${srcDir}/index`, `${destDir}/app.min.js`, options)
);

gulp.task('bundle:ignite:app-min:replace', () =>
    gulp.src('./build/index.html')
        .pipe(htmlReplace({
            css: 'app.min.css',
            js: 'app.min.js'
        }))
        .pipe(gulp.dest('./build'))
);
