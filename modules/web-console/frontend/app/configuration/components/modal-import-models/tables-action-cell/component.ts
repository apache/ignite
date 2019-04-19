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
import {Menu} from 'app/types';

const IMPORT_DM_NEW_CACHE = 1;

export class TablesActionCell {
    static $inject = ['$element'];

    constructor(private $element: JQLite) {}

    onEditStart?: ng.ICompiledExpression;
    onCacheSelect?: ng.ICompiledExpression;
    table: any;
    caches: Menu<string>;
    importActions: any;

    onClick(e: JQueryEventObject) {
        e.stopPropagation();
    }

    $postLink() {
        this.$element.on('click', this.onClick);
    }

    $onDestroy() {
        this.$element.off('click', this.onClick);
        this.$element = null;
    }

    tableActionView(table) {
        if (!this.caches)
            return;

        const cache = this.caches.find((c) => c.value === table.cacheOrTemplate);

        if (!cache)
            return;

        const cacheName = cache.label;

        if (table.action === IMPORT_DM_NEW_CACHE)
            return 'Create ' + table.generatedCacheName + ' (' + cacheName + ')';

        return 'Associate with ' + cacheName;
    }
}

export const component = {
    controller: TablesActionCell,
    bindings: {
        onEditStart: '&',
        onCacheSelect: '&?',
        table: '<',
        caches: '<',
        importActions: '<'
    },
    template
};
