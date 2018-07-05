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

import _ from 'lodash';

import {Observable} from 'rxjs/Observable';
import {Subject} from 'rxjs/Subject';
import 'rxjs/add/observable/race';
import 'rxjs/add/operator/filter';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/pluck';
import 'rxjs/add/operator/take';
import 'rxjs/add/operator/toPromise';

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
            console.timeEnd('Post message');
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

        return Observable.race(
            this.messages$.filter((e) => e.tid === tid).take(1).pluck('m', 'data'),
            this.errors$.filter((e) => e.tid === tid).take(1).map((e) => {
                throw e.e;
            }))
            .take(1).toPromise();
    }
}
