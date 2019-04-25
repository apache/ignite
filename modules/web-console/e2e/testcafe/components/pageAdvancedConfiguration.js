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

export const pageAdvancedConfiguration = {
    saveButton: Selector('.pc-form-actions-panel .btn-ignite').withText('Save'),
    clusterNavButton: Selector('.pca-menu-link[ui-sref="base.configuration.edit.advanced.cluster"]'),
    modelsNavButton: Selector('.pca-menu-link[ui-sref="base.configuration.edit.advanced.models"]'),
    cachesNavButton: Selector('.pca-menu-link[ui-sref="base.configuration.edit.advanced.caches"]'),
    igfsNavButton: Selector('.pca-menu-link[ui-sref="base.configuration.edit.advanced.igfs"]'),
    async save() {
        await t.click(this.saveButton);
    }
};
