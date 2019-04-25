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
import templateUrl from './template.tpl.pug';
import { NotebookCtrl } from './controller';
import NotebookData from '../../notebook.data';
import {component as actions} from './components/query-actions-button/component';
import {default as igniteInformation} from './components/ignite-information/information.directive';
import './style.scss';

export default angular.module('ignite-console.sql.notebook', [])
    .directive('igniteInformation', igniteInformation)
    .component('queryActionsButton', actions)
    .component('queriesNotebook', {
        controller: NotebookCtrl,
        templateUrl
    })
    .service('IgniteNotebookData', NotebookData);
