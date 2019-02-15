/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import Provider from './ConfigurationDownload';

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {spy} from 'sinon';

const mocks = () => new Map([
    ['messages', {
        showError: spy()
    }],
    ['activitiesData', {
        post: spy()
    }],
    ['configuration', {
        populate: (value) => Promise.resolve(value),
        _clusters: [],
        read() {
            return Promise.resolve({clusters: this._clusters});
        }
    }],
    ['summaryZipper', spy((value) => Promise.resolve(value))],
    ['Version', {
        currentSbj: {
            getValue() {
                return '2.0';
            }
        }
    }],
    ['$q', Promise],
    ['$rootScope', {
        IgniteDemoMode: true
    }],
    ['PageConfigure', {
        getClusterConfiguration: () => Promise.resolve({clusters: [{_id: 1, name: 'An Cluster'}]})
    }],
    ['IgniteConfigurationResource', {
        populate: () => Promise.resolve({clusters: []})
    }]
]);

const saverMock = () => ({
    saveAs: spy()
});

suite('page-configure, ConfigurationDownload service', () => {
    test('fails and shows error message when summary zipper fails', () => {
        const service = new Provider(...mocks().values());
        const cluster = {_id: 1, name: 'An Cluster'};
        service.configuration._clusters = [cluster];
        service.summaryZipper = () => Promise.reject({message: 'Summary zipper failed.'});

        return service.downloadClusterConfiguration(cluster)
        .then(() => Promise.reject('Should not happen'))
        .catch(() => {
            assert.equal(
                service.messages.showError.getCall(0).args[0],
                'Failed to generate project files. Summary zipper failed.',
                'shows correct error message when summary zipper fails'
            );
        });
    });

    test('calls correct dependcies', () => {
        const service = new Provider(...mocks().values());
        service.saver = saverMock();
        const cluster = {_id: 1, name: 'An Cluster'};
        service.configuration._clusters = [cluster];

        return service.downloadClusterConfiguration(cluster)
        .then(() => {
            assert.deepEqual(
                service.activitiesData.post.getCall(0).args[0],
                {action: '/configuration/download'},
                'submits activity data'
            );
            assert.deepEqual(service.summaryZipper.getCall(0).args, [{
                cluster,
                data: {},
                IgniteDemoMode: true,
                targetVer: '2.0'
            }], 'summary zipper arguments are correct');
            assert.deepEqual(service.saver.saveAs.getCall(0).args, [
                {
                    cluster,
                    data: {},
                    IgniteDemoMode: true,
                    targetVer: '2.0'
                },
                'An_Cluster-project.zip'
            ], 'saver arguments are correct');
        });
    });
});
