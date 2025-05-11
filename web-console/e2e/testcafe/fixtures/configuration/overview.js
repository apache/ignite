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

import {getLocationPathname, scrollIntoView, scrollToPageBottom} from '../../helpers';
import {dropTestDB, insertTestUser, resolveUrl} from '../../environment/envtools';
import {createRegularUser} from '../../roles';
import {PageConfigurationOverview} from '../../page-models/PageConfigurationOverview';
import {PageConfigurationBasic} from '../../page-models/PageConfigurationBasic';
import * as pageConfiguration from '../../components/pageConfiguration';
import {pageAdvancedConfiguration} from '../../components/pageAdvancedConfiguration';
import {PageConfigurationAdvancedCluster} from '../../page-models/PageConfigurationAdvancedCluster';
import {confirmation} from '../../components/confirmation';
import {successNotification} from '../../components/notifications';
import * as models from '../../page-models/pageConfigurationAdvancedModels';
import {configureNavButton} from '../../components/topNavigation';

const regularUser = createRegularUser();

const repeat = (times, fn) => [...Array(times).keys()].reduce((acc, i) => acc.then(() => fn(i)), Promise.resolve());

fixture('Configuration overview')
    .before(async(t) => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        await t.useRole(regularUser).navigateTo(resolveUrl(`/configuration/overview`));
    })
    .after(dropTestDB);

const overviewPage = new PageConfigurationOverview();
const basicConfigPage = new PageConfigurationBasic();
const advancedConfigPage = new PageConfigurationAdvancedCluster();

test('Create cluster basic/advanced clusters amount redirect', async(t) => {
    const clustersAmountThershold = 10;

    await repeat(clustersAmountThershold + 2, async(i) => {
        await t.click(overviewPage.createClusterConfigButton);

        if (i <= clustersAmountThershold) {
            await t.expect(getLocationPathname()).contains('basic', 'Opens basic');
            await basicConfigPage.saveWithoutDownload();
        } else {
            await t.expect(getLocationPathname()).contains('advanced', 'Opens advanced');
            await advancedConfigPage.save();
        }

        await t.click(configureNavButton);
    });
    await overviewPage.removeAllItems();
});

test('Cluster edit basic/advanced redirect based on caches amount', async(t) => {
    const clusterName = 'Seven caches cluster';
    const clusterEditLink = overviewPage.clustersTable.findCell(0, 'Name').find('a');
    const cachesAmountThreshold = 5;

    await t.click(overviewPage.createClusterConfigButton);
    await repeat(cachesAmountThreshold, async() => {
        await scrollToPageBottom();
        basicConfigPage.cachesList.addItem();
    });
    await basicConfigPage.saveWithoutDownload();
    await t
        .click(configureNavButton)
        .click(clusterEditLink)
        .expect(getLocationPathname()).contains('basic', `Opens basic with ${cachesAmountThreshold} caches`);
    await basicConfigPage.cachesList.addItem();
    await basicConfigPage.saveWithoutDownload();
    await scrollIntoView.with({dependencies: {el: configureNavButton}});
    await t
        .expect(configureNavButton.visible).ok()
        .click(configureNavButton.with({timeout: 0}))
        .click(clusterEditLink.with({timeout: 0}))
        .expect(getLocationPathname()).contains('advanced', `Opens advanced with ${cachesAmountThreshold + 1} caches`);
    await t.click(configureNavButton);
    await overviewPage.removeAllItems();
});

test('Cluster removal', async(t) => {
    const name = 'FOO bar BAZ';

    await t
        .click(overviewPage.createClusterConfigButton)
        .typeText(basicConfigPage.clusterNameInput.control, name, {replace: true});
    await basicConfigPage.saveWithoutDownload();
    await t.click(configureNavButton);
    await overviewPage.clustersTable.toggleRowSelection(1);
    await overviewPage.clustersTable.performAction('Delete');
    await t.expect(confirmation.body.textContent).contains(name, 'Lists cluster names in remove confirmation');
    await confirmation.confirm();
    await t.expect(successNotification.textContent).contains('Cluster(s) removed: 1', 'Shows cluster removal notification');
});

test('Cluster cell values', async(t) => {
    const name = 'Non-empty cluster config';
    const staticDiscovery = 'Static IPs';
    const cachesAmount = 3;
    const modelsAmount = 2;

    await t
        .resizeWindow(1200, 1080)
        .click(overviewPage.createClusterConfigButton)
        .typeText(basicConfigPage.clusterNameInput.control, name, {replace: true});
    await basicConfigPage.clusterDiscoveryInput.selectOption(staticDiscovery);
    await repeat(cachesAmount, () => basicConfigPage.cachesList.addItem());
    await basicConfigPage.saveWithoutDownload();
    await t
        .click(pageConfiguration.advancedNavButton)
        .click(pageAdvancedConfiguration.modelsNavButton);
    await repeat(modelsAmount, async(i) => {
        await t
            .click(models.createModelButton)
            .click(models.general.generatePOJOClasses.control);
        await models.general.queryMetadata.selectOption('Annotations');
        await t
            .typeText(models.general.keyType.control, `foo${i}`)
            .typeText(models.general.valueType.control, `bar${i}`)
            .click(pageAdvancedConfiguration.saveButton);
    });
    await t
        .click(configureNavButton)
        .expect(overviewPage.clustersTable.findCell(0, 'Name').textContent).contains(name)
        .expect(overviewPage.clustersTable.findCell(0, 'Discovery').textContent).contains(staticDiscovery)
        .expect(overviewPage.clustersTable.findCell(0, 'Caches').textContent).contains(cachesAmount)
        .expect(overviewPage.clustersTable.findCell(0, 'Models').textContent).contains(modelsAmount);
});
