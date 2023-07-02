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

import angular from 'angular';

import component from './component';
import listEditableCols from './components/list-editable-cols';
import transclude from './components/list-editable-transclude';
import listEditableOneWay from './components/list-editable-one-way';
import addItemButton from './components/list-editable-add-item-button';
import saveOnChanges from './components/list-editable-save-on-changes';

export default angular
    .module('ignite-console.list-editable', [
        addItemButton.name,
        listEditableCols.name,
        listEditableOneWay.name,
        transclude.name,
        saveOnChanges.name
    ])
    .component('listEditable', component);
