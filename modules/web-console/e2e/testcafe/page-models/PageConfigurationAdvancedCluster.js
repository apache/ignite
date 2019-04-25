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

import {Selector, t} from 'testcafe';
import {PanelCollapsible} from '../components/PanelCollapsible';
import {FormField} from '../components/FormField';

export class PageConfigurationAdvancedCluster {
    constructor() {
        this.saveButton = Selector('.pc-form-actions-panel .btn-ignite').withText('Save');

        this.sections = {
            connectorConfiguration: {
                panel: new PanelCollapsible('Connector configuration'),
                inputs: {
                    enable: new FormField({id: 'restEnabledInput'})
                }
            }
        };
    }

    async save() {
        await t.click(this.saveButton);
    }
}
