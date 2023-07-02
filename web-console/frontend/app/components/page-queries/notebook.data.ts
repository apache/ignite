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

import {DemoService} from 'app/modules/demo/Demo.module';
import _ from 'lodash';

const DEMO_NOTEBOOK = {
    name: 'SQL demo',
    id: 'demo',
    paragraphs: [
        {
            name: 'Query with refresh rate',
            queryType: 'SQL_FIELDS',
            pageSize: 100,
            query: [
                'SELECT count(*)',
                'FROM "CarCache".Car'
            ].join('\n'),
            result: 'BAR',
            timeLineSpan: 1,
            rate: {
                value: 3,
                unit: 1000,
                installed: true
            }
        },
        {
            name: 'Simple query',
            queryType: 'SQL_FIELDS',
            pageSize: 100,
            query: 'SELECT * FROM "CarCache".Car',
            result: 'TABLE',
            timeLineSpan: 1,
            rate: {
                value: 30,
                unit: 1000,
                installed: false
            }
        },
        {
            name: 'Query with aggregates',
            queryType: 'SQL_FIELDS',
            pageSize: 100,
            query: [
                'SELECT p.name, count(*) AS cnt',
                'FROM "ParkingCache".Parking p',
                'INNER JOIN "CarCache".Car c',
                '  ON (p.id) = (c.parkingId)',
                'GROUP BY P.NAME'
            ].join('\n'),
            result: 'TABLE',
            timeLineSpan: 1,
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
    static $inject = ['Demo', '$http', '$q', '$translate'];

    constructor(
        private Demo: DemoService,
        private $http: ng.IHttpService,
        private $q: ng.IQService,
        private $translate: ng.translate.ITranslateService
    ) {
        this.initLatch = null;
        this.notebooks = null;
    }

    demo = this.Demo.enabled;

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

    find(id) {
        return this.read()
            .then(() => {
                const notebook = this.demo ? this.notebooks[0] : _.find(this.notebooks, {id});

                if (_.isNil(notebook))
                    return this.$q.reject(this.$translate.instant('queries.failedToLoadNotebookErrorMessage'));

                return notebook;
            });
    }

    findIndex(notebook) {
        return this.read()
            .then(() => _.findIndex(this.notebooks, {id: notebook.id}));
    }

    save(notebook) {
        if (this.demo)
            return this.$q.when(DEMO_NOTEBOOK);

        return this.$http.put('/api/v1/notebooks', notebook)
            .then(() => {
                const idx = _.findIndex(this.notebooks, {id: notebook.id});

                if (idx < 0)
                    this.notebooks.push(notebook);
                else
                    this.notebooks[idx] = notebook;

                return notebook;
            })
            .catch(({data}) => Promise.reject(data));
    }

    remove(notebook) {
        if (this.demo) {
            return this.$q.reject(this.$translate.instant(
                'queries.notebookRemovalNotSupportedInDemoModeErrorMessage',
                {name: notebook.name}
            ));
        }

        const notebookId = notebook.id;

        return this.$http.delete(`/api/v1/notebooks/${notebookId}`)
            .then(() => {
                const idx = _.findIndex(this.notebooks, {id: notebookId});

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
