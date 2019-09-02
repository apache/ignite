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

import {service as GettingStartedServiceFactory} from 'app/modules/getting-started/GettingStarted.provider';

export default class HelpMenu {
    static $inject = ['gettingStarted'];

    constructor(private gettingStarted: ReturnType<typeof GettingStartedServiceFactory>) {}

    items = [
        {text: 'Getting Started', click: '$ctrl.gettingStarted.tryShow(true)'},
        {text: 'Documentation', href: 'https://docs.gridgain.com/docs/web-console', target: '_blank'},
        {text: 'Forums', href: 'https://forums.gridgain.com/home', target: '_blank'},
        {text: 'Support', href: 'https://gridgain.freshdesk.com/support/login', target: '_blank'},
        {text: 'Webinars', href: 'https://www.gridgain.com/resources/webinars', target: '_blank'},
        {text: 'Whitepapers', href: 'https://www.gridgain.com/resources/literature/white-papers', target: '_blank'}
    ];
}
