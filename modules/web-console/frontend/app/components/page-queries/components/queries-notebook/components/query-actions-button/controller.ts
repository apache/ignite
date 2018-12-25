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

export type QueryActions < T > = Array<{text: string, click?(item: T): any, available?(item: T): boolean}>;

export default class QueryActionButton<T> {
    static $inject = ['$element'];

    item: T;

    actions: QueryActions<T>;

    boundActions: QueryActions<undefined> = [];

    constructor(private el: JQLite) {}

    $postLink() {
        this.el[0].classList.add('btn-ignite-group');
    }

    $onChanges(changes: {actions: ng.IChangesObject<QueryActionButton<T>['actions']>}) {
        if ('actions' in changes) {
            this.boundActions = changes.actions.currentValue.map((a) => {
                const action = {...a};

                const click = () => a.click(this.item);

                Object.defineProperty(action, 'click', {
                    get: () => {
                        return typeof a.available === 'function'
                            ? a.available(this.item) ? click : void 0
                            : a.available ? click : void 0;
                    }
                });
                return action;
            });
        }
    }
}
