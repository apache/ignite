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

const srcDir = './app';
const destDir = './build';

const igniteModulesDir = process.env.IGNITE_MODULES ? path.normalize(process.env.IGNITE_MODULES) : './ignite_modules';
const igniteModulesTemp = './ignite_modules_temp';

const sassPaths = [
    './public/stylesheets/*.scss'
];

const jadePaths = [
    '!./views/error.jade',
    './views/*.jade',
    './views/**/*.jade'
];

const jadeModulePaths = [
    igniteModulesDir + '/**/view/**/*.jade'
];

const resourcePaths = [
    './controllers/**/*.json',
    './public/**/*.png',
    './public/*.ico'
];

const resourceModulePaths = [
    igniteModulesDir + '/**/controllers/models/*.json',
    igniteModulesDir + '/**/images/*.png'
];

const jsPaths = [
    './{app,controllers,generator}/*.js',
    './{app,controllers,generator}/**/*.js'
];

const jsModulePaths = [
    igniteModulesTemp + '/index.js',
    igniteModulesTemp + '/**/main.js',
    igniteModulesTemp + '/**/module.js',
    igniteModulesTemp + '/**/app/modules/*.js',
    igniteModulesTemp + '/**/app/modules/**/*.js',
    igniteModulesTemp + '/**/app/modules/**/*.jade'
];

const appPaths = [
    './app/**/*.сss',
    './app/**/*.jade',
    './app/data/*.json'
].concat(jsPaths);

const appModulePaths = [
    igniteModulesDir + '/index.js',
    igniteModulesDir + '/**/main.js',
    igniteModulesDir + '/**/module.js',
    igniteModulesDir + '/**/app/**/*.css',
    igniteModulesDir + '/**/app/data/*.json',
    igniteModulesDir + '/**/app/modules/*.js',
    igniteModulesDir + '/**/app/modules/**/*.js',
    igniteModulesDir + '/**/app/modules/**/*.jade'
];

export {
    srcDir,
    destDir,
    igniteModulesDir,
    igniteModulesTemp,

    sassPaths,

    jadePaths,
    jadeModulePaths,

    resourcePaths,
    resourceModulePaths,

    jsPaths,
    jsModulePaths,

    appPaths,
    appModulePaths
};
