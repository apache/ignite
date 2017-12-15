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

import 'rxjs/add/operator/partition';

import map from 'lodash/fp/map';

export default class {
    static $inject = ['IgniteMessages', 'IgniteNotebook', 'CreateQueryDialog', 'User', 'AgentManager'];

    constructor(Messages, queriesSrv, createQueryDialog, User, agentMgr) {
        Object.assign(this, { Messages, queriesSrv, createQueryDialog });

        this.text = 'Queries';

        if (agentMgr.isDemoMode())
            this.sref = 'base.sql.demo';
        else {
            this.click = () => this.createQueryDialog.show();
            this.getData();
        }

        User.current$
            .do((user) => this.hidden = user.becomeUsed)
            .subscribe();
    }

    getData() {
        [this.notEmptyList$, this.emptyList$] = this.queriesSrv.list$
            .partition((data) => data && data.length);

        this.emptyQueriesList$ = this.emptyList$
            .do(() => this.children = null)
            .subscribe(() => {});

        this.notEmptyQueriesList$ = this.notEmptyList$
            .do(() => {
                this.children = [
                    {text: 'Create new notebook', click: () => {
                        this.createQueryDialog.show();
                    }},
                    {divider: true}
                ];
            })
            .map(map((query) => ({
                data: query,
                action: {
                    icon: 'fa-trash',
                    click: (item) => this.queriesSrv.remove(item)
                },
                text: query.name,
                sref: `base.sql.notebook({noteId:"${query._id}"})`
            })))
            .do((data) => this.children.push(...data))
            .subscribe(() => {});
    }
}
