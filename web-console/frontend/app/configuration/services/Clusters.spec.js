

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {spy} from 'sinon';

import Provider from './Clusters';

const mocks = () => new Map([
    ['$http', {
        post: spy()
    }]
]);

suite('Clusters service', () => {
    test('discoveries', () => {
        const s = new Provider(...mocks().values());
        assert.isArray(s.discoveries, 'has discoveries array');
        assert.isOk(s.discoveries.every((d) => d.value && d.label), 'discoveries have correct format');
    });    

    test('getBlankCluster', () => {
        const s = new Provider(...mocks().values());
        assert.isObject(s.getBlankCluster());
    });
});
