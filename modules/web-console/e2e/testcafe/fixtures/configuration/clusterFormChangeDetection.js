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

import {dropTestDB, insertTestUser, resolveUrl} from '../../environment/envtools';
import {createRegularUser} from '../../roles';
import {PageConfigurationOverview} from '../../page-models/PageConfigurationOverview';
import {PageConfigurationAdvancedCluster} from '../../page-models/PageConfigurationAdvancedCluster';
import {advancedNavButton} from '../../components/pageConfiguration';
import {pageAdvancedConfiguration} from '../../components/pageAdvancedConfiguration';
import {confirmation} from '../../components/confirmation';
import {scrollIntoView} from '../../helpers';

const regularUser = createRegularUser();

fixture('Cluster configuration form change detection')
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        await t.useRole(regularUser);
    })
    .after(dropTestDB);

test('New cluster change detection', async(t) => {
    const overview = new PageConfigurationOverview();
    const advanced = new PageConfigurationAdvancedCluster();

    await t
        .navigateTo(resolveUrl(`/configuration/overview`))
        .click(overview.createClusterConfigButton)
        .click(advancedNavButton)
        .click(advanced.sections.connectorConfiguration.panel.heading);

    await scrollIntoView.with({dependencies: {el: advanced.sections.connectorConfiguration.panel.heading}})();

    await t
        .click(advanced.sections.connectorConfiguration.inputs.enable.control)
        .click(advanced.saveButton)
        .click(pageAdvancedConfiguration.cachesNavButton)
        .expect(confirmation.body.exists).notOk(`Doesn't show changes confiramtion after saving new cluster`);
});
