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

import {dropTestDB, insertTestUser, resolveUrl} from '../environment/envtools';
import {createRegularUser} from '../roles';
import * as helpMenu from '../components/helpMenu';
import {userMenu} from '../components/userMenu';
import * as gettingStarted from '../components/gettingStartedDialog';

const user = createRegularUser();

fixture('Help menu')
    .beforeEach(async(t) => {
        await dropTestDB();
        await insertTestUser();
        await t.useRole(user);
    })
    .afterEach(async(t) => {
        await dropTestDB();
    });

test.page(resolveUrl('/'))('Help menu items', async(t) => {
    const links = [
        {label: 'Documentation', href: 'https://docs.gridgain.com/docs/web-console'},
        {label: 'Forums', href: 'https://forums.gridgain.com/home'},
        {label: 'Support', href: 'https://gridgain.freshdesk.com/support/login'},
        {label: 'Webinars', href: 'https://www.gridgain.com/resources/webinars/search?combine=web+console&field_personas_target_id=All'},
        {label: 'Whitepapers', href: 'https://www.gridgain.com/resources/literature/white-papers?combine=web+console&field_personas_target_id=All'}
    ];
    await t.hover(helpMenu.trigger);
    for await (const link of links) {
        await t
            .expect(helpMenu.item(link.label).getAttribute('href')).eql(link.href)
            .expect(helpMenu.item(link.label).getAttribute('target')).eql('_blank');
    }
    await t
        .click(helpMenu.item('Getting Started'))
        .expect(gettingStarted.dialog.exists).ok()
        .click(gettingStarted.closeButton);
    await t
        .hover(userMenu.button)
        .expect(userMenu._selector.find('a').withText('Getting started').exists)
        .notOk('Getting started was moved to help menu');
});
