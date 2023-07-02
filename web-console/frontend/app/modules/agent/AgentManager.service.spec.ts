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

import {suite, test} from 'mocha';
import {assert} from 'chai';

import AgentManager from './AgentManager.service';
import {ClusterStats, IgniteFeatures} from 'app/types/Agent';

suite('Agent manager service', () => {
    test('allFeatures', () => {
        const cluster: ClusterStats  = {supportedFeatures: '+/l9'};

        const service = new AgentManager();

        const features = service.allFeatures(cluster);

        assert.ok(features.includes(IgniteFeatures.INDEXING));
        assert.ok(features.includes(IgniteFeatures.WC_SNAPSHOT_CHAIN_MODE));
        assert.ok(features.includes(IgniteFeatures.WC_ROLLING_UPGRADE_STATUS));
    });

    test('noFeaturesOnOldNode', () => {
        const cluster: ClusterStats  = {supportedFeatures: null};

        const service = new AgentManager();

        const features = service.allFeatures(cluster);

        assert.isNotOk(features.includes(IgniteFeatures.INDEXING));
        assert.isNotOk(features.includes(IgniteFeatures.WC_SNAPSHOT_CHAIN_MODE));
        assert.isNotOk(features.includes(IgniteFeatures.WC_ROLLING_UPGRADE_STATUS));
    });
});
