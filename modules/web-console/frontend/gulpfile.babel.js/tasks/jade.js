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
import ll from 'gulp-ll';
import jade from 'gulp-jade';

import { jadeViewsPaths, jadeAppModulePaths, jadeModulePaths, destDir, rootDir } from '../paths';

const jadeOptions = {
    basedir: rootDir,
    cache: true
};

ll.tasks(['jade:views', 'jade:app', 'jade:ignite_modules']);

gulp.task('jade', ['jade:views', 'jade:app', 'jade:ignite_modules']);

gulp.task('jade:views', () =>
    gulp.src(jadeViewsPaths)
        .pipe(jade(jadeOptions))
        .pipe(gulp.dest(destDir))
);

gulp.task('jade:app', () =>
    gulp.src(jadeAppModulePaths)
        .pipe(jade(jadeOptions))
        .pipe(gulp.dest(destDir))
);

gulp.task('jade:ignite_modules', () =>
    gulp.src(jadeModulePaths)
        .pipe(jade(jadeOptions))
        .pipe(gulp.dest(`${destDir}/ignite_modules`))
);
