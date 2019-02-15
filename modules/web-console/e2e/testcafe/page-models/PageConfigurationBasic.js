/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import {Selector, t} from 'testcafe';
import {FormField} from '../components/FormField';
import {ListEditable} from '../components/ListEditable';

class VersionPicker {
    constructor() {
        this._selector = Selector('version-picker');
    }
    /**
     * @param {string} label Version label
     */
    pickVersion(label) {
        return t
            .hover(this._selector)
            .click(this._selector.find('[role="menuitem"]').withText(label));
    }
}

export class PageConfigurationBasic {
    static SAVE_CHANGES_AND_DOWNLOAD_LABEL = 'Save and Download';
    static SAVE_CHANGES_LABEL = 'Save';

    constructor() {
        this._selector = Selector('page-configure-basic');
        this.versionPicker = new VersionPicker();
        this.totalOffheapSizeInput = Selector('form-field-size#memory');
        this.mainFormAction = Selector('.pc-form-actions-panel .btn-ignite-group .btn-ignite:nth-of-type(1)');
        this.contextFormActionsButton = Selector('.pc-form-actions-panel .btn-ignite-group .btn-ignite:nth-of-type(2)');
        this.contextSaveButton = Selector('a[role=menuitem]').withText(new RegExp(`^${PageConfigurationBasic.SAVE_CHANGES_LABEL}$`));
        this.contextSaveAndDownloadButton = Selector('a[role=menuitem]').withText(PageConfigurationBasic.SAVE_CHANGES_AND_DOWNLOAD_LABEL);
        this.buttonPreviewProject = Selector('button-preview-project');
        this.clusterNameInput = new FormField({id: 'clusterNameInput'});
        this.clusterDiscoveryInput = new FormField({id: 'discoveryInput'});
        this.cachesList = new ListEditable(Selector('.pcb-caches-list'), {
            name: {id: 'nameInput'},
            cacheMode: {id: 'cacheModeInput'},
            atomicityMode: {id: 'atomicityModeInput'},
            backups: {id: 'backupsInput'}
        });
        this.pageHeader = Selector('.header-with-selector h1');
    }

    async save() {
        await t.click(this.mainFormAction);
    }

    async saveWithoutDownload() {
        return await t.click(this.contextFormActionsButton).click(this.contextSaveButton);
    }
}
