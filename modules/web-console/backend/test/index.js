/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const Mocha = require('mocha');
const glob = require('glob');

const mocha = new Mocha({ui: 'tdd', reporter: process.env.MOCHA_REPORTER || 'spec'});
const testPath = ['./test/unit/**/*.js', './test/routes/**/*.js'];

testPath
    .map((mask) => glob.sync(mask))
    .reduce((acc, items) => acc.concat(items), [])
    .map(mocha.addFile.bind(mocha));

const runner = mocha.run();

runner.on('end', (failures) => process.exit(failures));
