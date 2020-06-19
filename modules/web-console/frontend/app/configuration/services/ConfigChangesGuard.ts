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

import {of} from 'rxjs';
import {switchMap, catchError} from 'rxjs/operators';
import {Confirm} from 'app/services/Confirm.service';
import {DiffPatcher} from 'jsondiffpatch';
import {html} from 'jsondiffpatch/public/build/jsondiffpatch-formatters.js';
import 'jsondiffpatch/public/formatters-styles/html.css';

export class IgniteObjectDiffer<T> {
    diffPatcher: DiffPatcher;

    constructor() {
        this.diffPatcher = new DiffPatcher({
            cloneDiffValues: true
        });

        const shouldSkip = (val) => val === null || val === void 0 || val === '';

        function igniteConfigFalsyFilter(context) {
            // Special logic for checkboxes.
            if (shouldSkip(context.left) && context.right === false)
                delete context.right;

            if (shouldSkip(context.left))
                delete context.left;

            if (shouldSkip(context.right))
                delete context.right;
        }

        igniteConfigFalsyFilter.filterName = 'igniteConfigFalsy';

        this.diffPatcher.processor.pipes.diff.before('trivial', igniteConfigFalsyFilter);
    }

    diff(a: T, b: T) {
        return this.diffPatcher.diff(a, b);
    }
}

export default class ConfigChangesGuard<T> {
    static $inject = ['Confirm', '$sce'];

    constructor(private Confirm: Confirm, private $sce: ng.ISCEService) {}

    differ = new IgniteObjectDiffer<T>();

    _hasChanges(a: T, b: T) {
        return this.differ.diff(a, b);
    }

    _confirm(changes) {
        return this.Confirm.confirm(this.$sce.trustAsHtml(`
            <p>
            You have unsaved changes.
            Are you sure you want to discard them?
            </p>
            <details class='config-changes-guard__details'>
                <summary>Click here to see changes</summary>
                <div style='max-height: 400px; overflow: auto;'>${html.format(changes)}</div>                
            </details>
        `));
    }

    /**
     * Compares values and asks user if he wants to continue.
     *
     * @param a Left comparison value.
     * @param b Right comparison value.
     */
    guard(a: T, b: T) {
        if (!a && !b)
            return Promise.resolve(true);

        return of(this._hasChanges(a, b)).pipe(
            switchMap((changes) => changes ? this._confirm(changes).then(() => true) : of(true)),
            catchError(() => of(false))
        ).toPromise();
    }
}
