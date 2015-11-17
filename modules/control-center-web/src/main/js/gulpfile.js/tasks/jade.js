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
var jade = require('gulp-jade');
var sequence = require('gulp-sequence');

var paths = [
    '!./views/error.jade',
    './views/*.jade',
    './views/**/*.jade'
];

var pluginPaths = [
    './ignite_modules/**/*.jade'
];

var options = {
};

gulp.task('jade', function() {
    return sequence(
        gulp.src(paths).pipe(jade(options)).pipe(gulp.dest('./build')),
        gulp.src(pluginPaths).pipe(jade(options)).pipe(gulp.dest('./build/ignite_modules'))
    );
});

gulp.task('jade:watch', function () {
    return gulp.watch(paths, function(e) {
        sequence('jade', 'inject:plugins:html')()
    });
});
