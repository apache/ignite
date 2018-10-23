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

import JavaTypes from '../../app/services/JavaTypes.service';

import generator from '../../app/modules/configuration/generator/ConfigurationGenerator';
import transformer from '../../app/modules/configuration/generator/SpringTransformer.service';

import { suite, test } from 'mocha';

suite.skip('SpringTransformerTestsSuite', () => {
    test('AtomicConfiguration', () => {
        const ConfigurationGenerator = generator[1]();
        const SpringTransformer = transformer[1][2](JavaTypes[1](), ConfigurationGenerator);

        const acfg = {
            atomicSequenceReserveSize: 1001,
            backups: 1,
            cacheMode: 'LOCAL'
        };

        const bean = ConfigurationGenerator.clusterAtomics(acfg);

        console.log(SpringTransformer.generateSection(bean));
    });

    test('IgniteConfiguration', () => {
        const ConfigurationGenerator = generator[1]();
        const SpringTransformer = transformer[1][2](JavaTypes[1](), ConfigurationGenerator);

        const cfg = {
            atomics: {
                atomicSequenceReserveSize: 1001,
                backups: 1,
                cacheMode: 'LOCAL'
            }
        };

        const bean = ConfigurationGenerator.igniteConfiguration(cfg);

        console.log(SpringTransformer.generate(bean));
    });
});
