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
import util from 'gulp-util';
import cache from 'gulp-cached';
import sequence from 'gulp-sequence';

import { destDir, igniteModulesDir, igniteModulesTemp } from '../paths';

const paths = [
    './app/**/**/*.js',
    './controllers/*.js',
    './controllers/**/*.js',
    './helpers/*.js',
    './helpers/**/*.js'
];

const resourcePaths = [
    './controllers/**/*.json',
    './public/**/*.png',
    './public/*.ico'
];

const igniteModulePaths = [
    igniteModulesTemp + '/**/main.js',
    igniteModulesTemp + '/**/module.js',
    igniteModulesTemp + '/**/app/modules/*.js',
    igniteModulesTemp + '/**/app/modules/**/*.js',
    igniteModulesTemp + '/**/controllers/*.js'
];

const igniteModuleResourcePaths = [
    igniteModulesDir + '/**/controllers/models/*.json',
    igniteModulesDir + '/**/images/*.png'
];

gulp.task('copy', function(cb) {
    const tasks = ['copy:resource', 'copy:ignite_modules:resource'];

    if (util.env.debug || util.env.sourcemaps) {
        tasks.push('copy:js');

        tasks.push('copy:ignite_modules:js');
    }

    return sequence(tasks, cb);
});

gulp.task('copy:js', () =>
    gulp.src(paths, {base: './'})
        .pipe(cache('copy:js'))
        .pipe(gulp.dest(destDir))
);

gulp.task('copy:resource', () =>
    gulp.src(resourcePaths)
        .pipe(gulp.dest(destDir))
);

gulp.task('copy:ignite_modules:js', () =>
    gulp.src(igniteModulePaths)
        .pipe(cache('copy:ignite_modules:js'))
        .pipe(gulp.dest(`${destDir}/${igniteModulesTemp}`))
);

gulp.task('copy:ignite_modules:resource', () =>
    gulp.src(igniteModuleResourcePaths)
        .pipe(gulp.dest(`${destDir}/ignite_modules`))
);

gulp.task('copy:watch', (cb) => {
    gulp.watch([resourcePaths, igniteModuleResourcePaths], ['copy:resource', 'copy:ignite_modules:resource']);

    gulp.watch([paths, igniteModulePaths], sequence('copy:js', 'copy:ignite_modules:js', 'bundle:ignite:app' , cb));
});
