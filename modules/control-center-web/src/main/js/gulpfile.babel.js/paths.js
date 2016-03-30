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

const root = path.dirname(__dirname);

const srcDir = path.join(root, 'app');
const destDir = path.join(root, 'build');

const igniteModulesDir = process.env.IGNITE_MODULES ? path.normalize(process.env.IGNITE_MODULES) : path.join(root, 'ignite_modules');
const igniteModulesTemp = path.join(root, 'ignite_modules_temp');

export {
    root,
    srcDir,
    destDir,
    igniteModulesDir,
    igniteModulesTemp
};
