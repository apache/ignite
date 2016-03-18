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
var jspm = require('jspm');
var util = require('gulp-util');
var concat = require('gulp-concat');
var sequence = require('gulp-sequence');

var paths = [
	'./app/**/*.js',
	'./app/**/*.jade',
	'./app/data/*.json'
];

var legacy_paths = [
	'!./controllers/common-module.js',
	'./controllers/*.js',
	'./controllers/**/*.js',
	'./helpers/generator/*.js',
	'./helpers/generator/**/*.js'
];

var options = {
	minify: true
};

gulp.task('bundle', ['eslint', 'bundle:ignite', 'bundle:legacy']);

// Package all external dependencies and ignite-console.
gulp.task('bundle:ignite', function() {
	if (util.env.debug)
		delete options.minify;

	if (util.env.debug || util.env.sourcemaps)
		options.sourceMaps = true;

	return jspm.bundleSFX('app/index', 'build/app.min.js', options);
});

// Package controllers and generators.
gulp.task('bundle:legacy', function() {
	return gulp.src(legacy_paths)
		.pipe(concat('all.js'))
		.pipe(gulp.dest('./build'));
});

gulp.task('bundle:ignite:watch', function() {
	return gulp.watch(paths, ['bundle:ignite']);
});

gulp.task('bundle:legacy:watch', function() {
    return gulp.watch(legacy_paths, ['bundle:legacy']);
});
