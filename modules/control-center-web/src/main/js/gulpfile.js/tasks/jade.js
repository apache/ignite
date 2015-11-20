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

var gulp = require('gulp');
var gulpJade = require('gulp-jade');
var gulpSequence = require('gulp-sequence');

var igniteModules = process.env.IGNITE_MODULES || './ignite_modules';

var paths = [
    '!./views/error.jade',
    './views/*.jade',
    './views/**/*.jade'
];

var igniteModulePaths = [
    igniteModules + '/**/view/**/*.jade'
];

var jadeOptions = {
    basedir: './'
};

gulp.task('jade', function(cb) {
    return gulpSequence('jade:source', 'jade:ignite_modules')(cb)
});

gulp.task('jade:source', function (cb) {
    return gulp.src(paths).pipe(gulpJade(jadeOptions)).pipe(gulp.dest('./build'));
});

gulp.task('jade:ignite_modules', function (cb) {
    return gulp.src(igniteModulePaths).pipe(gulpJade(jadeOptions)).pipe(gulp.dest('./build/ignite_modules'));
});

gulp.task('jade:watch', function (cb) {
    return gulp.watch([igniteModulePaths, paths], function(glob) {
        gulpSequence('jade', 'inject:plugins:html')(cb)
    });
});
