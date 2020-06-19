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
