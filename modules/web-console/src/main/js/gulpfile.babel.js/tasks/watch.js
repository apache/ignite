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
import sequence from 'gulp-sequence';

import { sassPaths, jadePaths, jadeModulePaths, resourcePaths, resourceModulePaths, appPaths, appModulePaths } from '../paths';

gulp.task('watch:sass', (cb) => sequence('sass', 'bundle:ignite:app', cb));

gulp.task('watch:ignite-modules', (cb) => sequence('clean:ignite-modules-temp', 'ignite:modules', ['eslint:browser', 'copy:ignite_modules:js', 'bundle:ignite:app'], cb));

// Build + connect + watch task.
gulp.task('watch', ['build', 'connect'], () => {
    gulp.watch(sassPaths, ['watch:sass']);

    gulp.watch(jadePaths.concat(jadeModulePaths), ['jade']);

    gulp.watch(resourcePaths, ['copy:resource']);
    gulp.watch(resourceModulePaths, ['copy:ignite_modules:resource']);

    gulp.watch(appPaths, ['eslint:browser', 'copy:js', 'bundle:ignite:app']);
    gulp.watch(appModulePaths, ['watch:ignite-modules']);
});

