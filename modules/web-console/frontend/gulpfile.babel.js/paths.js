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

import path from 'path';

const rootDir = path.resolve('./');
const srcDir = path.resolve('app');
const destDir = path.resolve('build');

const igniteModulesDir = process.env.IGNITE_MODULES ? path.join(path.normalize(process.env.IGNITE_MODULES), 'frontend') : './ignite_modules';
const igniteModulesTemp = path.resolve('ignite_modules_temp');

const jadeViewsPaths = [
    './views/**/*.jade',
    '!./views/configuration/*.jade'
];

const jadeAppModulePaths = [
    './app/modules/states/configuration/**/*.jade',
    './app/modules/sql/*.jade',
    './views/**/*.jade',
    '!./views/*.jade',
    '!./views/includes/*.jade',
    '!./views/settings/*.jade',
    '!./views/sql/*.jade',
    '!./views/templates/*.jade'
];

const jadeModulePaths = [
    igniteModulesDir + '/**/view/**/*.jade'
];

const appModulePaths = [
    igniteModulesDir + '/index.js',
    igniteModulesDir + '/**/main.js',
    igniteModulesDir + '/**/module.js',
    igniteModulesDir + '/**/app/modules/*.js',
    igniteModulesDir + '/**/app/modules/**/*.js',
    igniteModulesDir + '/**/app/modules/**/*.jade',
    igniteModulesDir + '/**/app/**/*.css',
    igniteModulesDir + '/**/app/**/*.scss',
    igniteModulesDir + '/**/app/data/*.json'
];

const resourcePaths = [
    './public/**/*.png',
    './public/*.ico'
];

const resourceModulePaths = [
    igniteModulesDir + '/**/images/*.png',
    igniteModulesDir + '/*.ico'
];

export {
    rootDir,
    srcDir,
    destDir,
    igniteModulesDir,
    igniteModulesTemp,

    jadeViewsPaths,
    jadeAppModulePaths,
    jadeModulePaths,

    resourcePaths,
    resourceModulePaths,
    appModulePaths
};
