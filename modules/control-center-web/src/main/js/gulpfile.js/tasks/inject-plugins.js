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
var inject = require('gulp-inject');

var common_options = {
    read: false
};

var html_targets = [
    './build/index.html'
];

var js_targets = [
    './build/common-module.js'
];

var js_sources = [
    './build/ignite_modules/**/main.js'
];

var html_sources = [
    './build/ignite_modules/**/main.js',
    './build/ignite_modules/**/app/modules/*.js',
    './build/ignite_modules/**/app/modules/*.js',
    './build/ignite_modules/**/app/modules/**/*.js'
];

gulp.task('inject:plugins:html', function() {
    gulp.src(html_targets)
        .pipe(inject(gulp.src(html_sources, common_options), {
            starttag: '<!-- ignite:plugins-->',
            endtag: '<!-- endignite-->',
            transform: function (filePath, file, i, length) {
                // return file contents as string
                return '<script src="' + filePath.replace(/\/build(.*)/mgi, '$1') + '"></script>';
            }
        }))
        .pipe(gulp.dest('./build'));
});

gulp.task('inject:plugins:js', function() {
    gulp.src(js_targets)
        .pipe(inject(gulp.src(js_sources, common_options), {
            starttag: '/* ignite:plugins */',
            endtag: ' /* endignite */',
                transform: function (filePath, file, i, length) {
                // return file contents as string
                return ", 'ignite-console." + filePath.replace(/.*ignite_modules\/([^\/]+).*/mgi, '$1') + "'";
            }
        }))
        .pipe(gulp.dest('./build'));
});

gulp.task('inject:plugins', ['inject:plugins:html', 'inject:plugins:js']);
