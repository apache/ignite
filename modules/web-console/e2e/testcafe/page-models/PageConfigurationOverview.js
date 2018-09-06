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

import {Selector, t} from 'testcafe';
import {Table} from '../components/Table';
import {confirmation} from '../components/confirmation';
import {successNotification} from '../components/notifications';

export class PageConfigurationOverview {
    constructor() {
        this.createClusterConfigButton = Selector('.btn-ignite').withText('Create Cluster Configuration');
        this.importFromDBButton = Selector('.btn-ignite').withText('Import from Database');
        this.clustersTable = new Table(Selector('pc-items-table'));
        this.pageHeader = Selector('.pc-page-header');
    }
    async removeAllItems() {
        await t.click(this.clustersTable.allItemsCheckbox);
        await this.clustersTable.performAction('Delete');
        await confirmation.confirm();
        await t.expect(successNotification.visible).ok();
    }
}
