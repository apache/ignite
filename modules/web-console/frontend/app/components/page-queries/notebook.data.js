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

const DEMO_NOTEBOOK = {
    name: 'SQL demo',
    _id: 'demo',
    paragraphs: [
        {
            name: 'Query with refresh rate',
            qryType: 'query',
            pageSize: 100,
            limit: 0,
            query: [
                'SELECT count(*)',
                'FROM "CarCache".Car'
            ].join('\n'),
            result: 'bar',
            timeLineSpan: '1',
            rate: {
                value: 3,
                unit: 1000,
                installed: true
            }
        },
        {
            name: 'Simple query',
            qryType: 'query',
            pageSize: 100,
            limit: 0,
            query: 'SELECT * FROM "CarCache".Car',
            result: 'table',
            timeLineSpan: '1',
            rate: {
                value: 30,
                unit: 1000,
                installed: false
            }
        },
        {
            name: 'Query with aggregates',
            qryType: 'query',
            pageSize: 100,
            limit: 0,
            query: [
                'SELECT p.name, count(*) AS cnt',
                'FROM "ParkingCache".Parking p',
                'INNER JOIN "CarCache".Car c',
                '  ON (p.id) = (c.parkingId)',
                'GROUP BY P.NAME'
            ].join('\n'),
            result: 'table',
            timeLineSpan: '1',
            rate: {
                value: 30,
                unit: 1000,
                installed: false
            }
        }
    ],
    expandedParagraphs: [0, 1, 2]
};

export default class NotebookData {
    static $inject = ['$rootScope', '$http', '$q'];

    /**
     * @param {ng.IRootScopeService} $root 
     * @param {ng.IHttpService} $http 
     * @param {ng.IQService} $q    
     */
    constructor($root, $http, $q) {
        this.demo = $root.IgniteDemoMode;

        this.initLatch = null;
        this.notebooks = null;

        this.$http = $http;
        this.$q = $q;
    }

    load() {
        if (this.demo) {
            if (this.initLatch)
                return this.initLatch;

            return this.initLatch = this.$q.when(this.notebooks = [DEMO_NOTEBOOK]);
        }

        return this.initLatch = this.$http.get('/api/v1/notebooks')
            .then(({data}) => this.notebooks = data)
            .catch(({data}) => Promise.reject(data));
    }

    read() {
        if (this.initLatch)
            return this.initLatch;

        return this.load();
    }

    find(_id) {
        return this.read()
            .then(() => {
                const notebook = this.demo ? this.notebooks[0] : _.find(this.notebooks, {_id});

                if (_.isNil(notebook))
                    return this.$q.reject('Failed to load notebook.');

                return notebook;
            });
    }

    findIndex(notebook) {
        return this.read()
            .then(() => _.findIndex(this.notebooks, {_id: notebook._id}));
    }

    save(notebook) {
        if (this.demo)
            return this.$q.when(DEMO_NOTEBOOK);

        return this.$http.post('/api/v1/notebooks/save', notebook)
            .then(({data}) => {
                const idx = _.findIndex(this.notebooks, {_id: data._id});

                if (idx >= 0)
                    this.notebooks[idx] = data;
                else
                    this.notebooks.push(data);

                return data;
            })
            .catch(({data}) => Promise.reject(data));
    }

    remove(notebook) {
        if (this.demo)
            return this.$q.reject(`Removing "${notebook.name}" notebook is not supported.`);

        const key = {_id: notebook._id};

        return this.$http.post('/api/v1/notebooks/remove', key)
            .then(() => {
                const idx = _.findIndex(this.notebooks, key);

                if (idx >= 0) {
                    this.notebooks.splice(idx, 1);

                    if (idx < this.notebooks.length)
                        return this.notebooks[idx];
                }

                if (this.notebooks.length > 0)
                    return this.notebooks[this.notebooks.length - 1];

                return null;
            })
            .catch(({data}) => Promise.reject(data));
    }
}
