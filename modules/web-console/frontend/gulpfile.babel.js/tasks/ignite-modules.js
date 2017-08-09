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
import inject from 'gulp-inject';
import clean from 'gulp-rimraf';
import sequence from 'gulp-sequence';
import {appModulePaths, igniteModulesTemp} from '../paths';

gulp.task('ignite:modules', (cb) => sequence('ignite:modules:copy', 'ignite:modules:inject', cb));

gulp.task('ignite:modules:copy', () =>
    gulp.src(appModulePaths)
        .pipe(gulp.dest(igniteModulesTemp))
);

gulp.task('ignite:modules:inject', () =>
    gulp.src(`${igniteModulesTemp}/index.js`)
        .pipe(inject(gulp.src([`${igniteModulesTemp}/**/main.js`]), {
            starttag: '/* ignite:modules */',
            endtag: '/* endignite */',
            transform: (filePath) => {
                const igniteModuleName = filePath.replace(/.*ignite_modules_temp\/([^\/]+).*/mgi, '$1');

                // Return file contents as string.
                return `import './${igniteModuleName}/main';`;
            }
        }))
        .pipe(inject(gulp.src([`${igniteModulesTemp}/**/main.js`]), {
            starttag: '/* ignite-console:modules */',
            endtag: '/* endignite */',
            transform: (filePath, file, i) => {
                const igniteModuleName = filePath.replace(/.*ignite_modules_temp\/([^\/]+).*/mgi, '$1');

                // Return file contents as string.
                return (i ? ',' : '') + `'ignite-console.${igniteModuleName}'`;
            }
        }))
        .pipe(clean({force: true}))
        .pipe(gulp.dest(igniteModulesTemp))
);
