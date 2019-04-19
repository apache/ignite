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

import template from './template.pug';
import './style.scss';

export class Breadcrumbs {
    static $inject = ['$transclude', '$element'];
    /**
     * @param {ng.ITranscludeFunction} $transclude
     * @param {JQLite} $element
     */
    constructor($transclude, $element) {
        this.$transclude = $transclude;
        this.$element = $element;
    }
    $postLink() {
        this.$transclude((clone) => {
            clone.first().prepend(this.$element.find('.breadcrumbs__home'));
            this.$element.append(clone);
        });
    }
}

export default {
    controller: Breadcrumbs,
    template,
    transclude: true
};
