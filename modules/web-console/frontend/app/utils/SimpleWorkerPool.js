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

import _ from 'lodash';
import {Subject, race} from 'rxjs';
import {filter, take, pluck, map} from 'rxjs/operators';

/**
 * Simple implementation of workers pool.
 */
export default class SimpleWorkerPool {
    constructor(name, WorkerClass, poolSize = (navigator.hardwareConcurrency || 4), dbg = false) {
        this._name = name;
        this._WorkerClass = WorkerClass;
        this._tasks = [];
        this._msgId = 0;
        this.messages$ = new Subject();
        this.errors$ = new Subject();
        this.__dbg = dbg;

        this._workers = _.range(poolSize).map(() => {
            const worker = new this._WorkerClass();

            worker.onmessage = (m) => {
                this.messages$.next({tid: worker.tid, m});

                worker.tid = null;

                this._run();
            };

            worker.onerror = (e) => {
                this.errors$.next({tid: worker.tid, e});

                worker.tid = null;

                this._run();
            };

            return worker;
        });
    }

    _makeTaskID() {
        return this._msgId++;
    }

    _getNextWorker() {
        return this._workers.find((w) => _.isNil(w.tid));
    }

    _getNextTask() {
        return this._tasks.shift();
    }

    _run() {
        const worker = this._getNextWorker();

        if (!worker || !this._tasks.length)
            return;

        const task = this._getNextTask();

        worker.tid = task.tid;

        if (this.__dbg)
            console.time(`Post message[pool=${this._name}]`);

        worker.postMessage(task.data);

        if (this.__dbg)
            console.timeEnd(`Post message[pool=${this._name}]`);
    }

    terminate() {
        this._workers.forEach((w) => w.terminate());

        this.messages$.complete();
        this.errors$.complete();

        this._workers = null;
    }

    postMessage(data) {
        const tid = this._makeTaskID();

        this._tasks.push({tid, data});

        if (this.__dbg)
            console.log(`Pool: [name=${this._name}, tid=${tid}, queue=${this._tasks.length}]`);

        this._run();

        return race(
            this.messages$.pipe(filter((e) => e.tid === tid), take(1), pluck('m', 'data')),
            this.errors$.pipe(filter((e) => e.tid === tid), take(1), map((e) => {
                throw e.e;
            }))
        ).pipe(take(1)).toPromise();
    }
}
