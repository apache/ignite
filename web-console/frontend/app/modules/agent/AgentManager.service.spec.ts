

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
