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
        this.totalOffheapSizeInput = Selector('pc-form-field-size#memory');
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
