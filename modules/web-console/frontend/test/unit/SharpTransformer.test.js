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

import generator from '../../app/modules/configuration/generator/PlatformGenerator';
import transformer from '../../app/modules/configuration/generator/SharpTransformer.service';

import { assert } from 'chai';

suite.skip('SharpTransformerTestsSuite', () => {
    test('AtomicConfiguration', () => {
        const PlatformGenerator = generator[1]();
        const SharpTransformer = transformer[1](PlatformGenerator);

        const acfg = {
            atomicSequenceReserveSize: 1001,
            backups: 1,
            cacheMode: 'LOCAL'
        };

        const bean = PlatformGenerator.clusterAtomics(acfg);

        console.log(SharpTransformer.generateSection(bean));
    });

    test('IgniteConfiguration', () => {
        const PlatformGenerator = generator[1]();
        const SharpTransformer = transformer[1](PlatformGenerator);

        const clusterCfg = {
            atomics: {
                atomicSequenceReserveSize: 1001,
                backups: 1,
                cacheMode: 'LOCAL'
            }
        };

        const bean = PlatformGenerator.igniteConfiguration(clusterCfg);

        console.log(SharpTransformer.toClassFile(bean, 'config', 'ServerConfigurationFactory', null));
    });
});
