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
var util = require('gulp-util');
var sequence = require('gulp-sequence');

var igniteModules = process.env.IGNITE_MODULES || './ignite_modules';

var paths = [
    './app/**/**/*.js'
];

var cssPaths = [
    'jspm_packages/**/nvd3@*/build/nv.d3.css',
    'jspm_packages/**/angular-tree-control@*/css/*.css',
    'jspm_packages/**/ag-grid@*/dist/ag-grid.css',
    'jspm_packages/**/angular-loading@*/angular-loading.css',
    'jspm_packages/**/angular-motion@*/dist/angular-motion.css'
];

var legacyPaths = [
    './controllers/*.js',
    './controllers/**/*.js',
    './controllers/**/*.json',
    './helpers/*.js',
    './helpers/**/*.js',
    './public/**/*.png',
    './public/*.png'
];

var igniteModulePaths = [
    igniteModules + '/**/main.js',
    igniteModules + '/**/app/modules/*.js',
    igniteModules + '/**/app/modules/**/*.js',
    igniteModules + '/**/controllers/*.js',
    igniteModules + '/**/controllers/models/*.json'
];

gulp.task('copy', function(cb) {
    var tasks = ['copy:legacy', 'copy:font-awesome', 'copy:ui-grid', 'copy:ignite_modules'];

    if (util.env.debug || util.env.sourcemaps) {
        tasks.push('copy:css');

        tasks.push('copy:base');
    }

    return sequence(tasks)(cb);
});

gulp.task('copy:base', function(cb) {
    return gulp.src(paths, {base: './'}).pipe(gulp.dest('./build'));
});

gulp.task('copy:legacy', function(cb) {
    return gulp.src(legacyPaths).pipe(gulp.dest('./build'));
});

gulp.task('copy:css', function(cb) {
    return gulp.src(cssPaths, {base: './'}).pipe(gulp.dest('./build'));
});

gulp.task('copy:font-awesome', function(cb) {
    return gulp.src('./node_modules/font-awesome/fonts/*', {base: './node_modules/font-awesome'}).pipe(gulp.dest('./build'));
});

gulp.task('copy:ui-grid', function(cb) {
    return gulp.src(['jspm_packages/**/*-ui-grid@*/*.woff',
        'jspm_packages/**/*-ui-grid@*/*.svg',
        'jspm_packages/**/*-ui-grid@*/*.ttf',
        'jspm_packages/**/*-ui-grid@*/*.eot'], {base: './'}).pipe(gulp.dest('./build'));
});

gulp.task('copy:ignite_modules', function(cb) {
    return gulp.src(igniteModulePaths).pipe(gulp.dest('./build/ignite_modules'));
});

gulp.task('copy:watch', function(cb) {
    gulp.watch([paths, legacyPaths, igniteModulePaths], function(glob) {
        sequence(['copy:base', 'copy:legacy', 'copy:ignite_modules'], 'inject:plugins:js')(cb);
    });
});
