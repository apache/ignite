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

import templateUrl from './template.tpl.pug';

export default {
    templateUrl,
    transclude: {
        queriesButtons: '?queriesButtons',
        queriesContent: '?queriesContent',
        queriesTitle: '?queriesTitle'
    },
    controller: class Ctrl {
        static $inject = ['$element', '$rootScope', '$state', 'IgniteNotebook'];

        /**
         * @param {JQLite} $element       
         * @param {ng.IRootScopeService} $rootScope     
         * @param {import('@uirouter/angularjs').StateService} $state         
         * @param {import('./notebook.service').default} IgniteNotebook
         */
        constructor($element, $rootScope, $state, IgniteNotebook) {
            this.$element = $element;
            this.$rootScope = $rootScope;
            this.$state = $state;
            this.IgniteNotebook = IgniteNotebook;
        }

        $onInit() {
            this.loadNotebooks();
        }

        async loadNotebooks() {
            const fetchNotebooksPromise = this.IgniteNotebook.read();

            this.notebooks = await fetchNotebooksPromise || [];
        }

        $postLink() {
            this.queriesTitle = this.$element.find('.queries-title');
            this.queriesButtons = this.$element.find('.queries-buttons');
            this.queriesContent = this.$element.find('.queries-content');
        }
    }
};
