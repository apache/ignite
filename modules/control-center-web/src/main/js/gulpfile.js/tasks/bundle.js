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

var paths = [
    './app/**/*.js'
];

var options = {
	minify: true
};

gulp.task('bundle', function() {
	if (util.env.debug) {
		delete options.minify;
	}

	if (util.env.debug || util.env.sourcemaps) {
		options.sourceMaps = true;
	}

	return jspm.bundleSFX('app/index', 'build/app.min.js', options);
});

gulp.task('bundle:watch', function() {
	gulp.watch(paths, ['bundle'])
});
