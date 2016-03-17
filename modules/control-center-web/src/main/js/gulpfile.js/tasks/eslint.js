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
var cache = require('gulp-cached');
var sequence = require('gulp-sequence');

var eslint = require('gulp-eslint');

var paths = [
    './app/**/*.js'
];

gulp.task('eslint:node', function() {
	return gulp.src('./serve/**/*.js')
        .pipe(cache('eslint:node'))
		.pipe(eslint({envs: ['node']}))
		.pipe(eslint.format());
});

gulp.task('eslint:browser', function() {
	return gulp.src(paths)
        .pipe(cache('eslint:browser'))
		.pipe(eslint({envs: ['browser']}))
		.pipe(eslint.format());
});

gulp.task('eslint', function(cb) {
	return sequence('eslint:browser', 'eslint:node')(cb);
});
