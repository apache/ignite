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

import template from './template.pug';
import './style.scss';

const IMPORT_DM_NEW_CACHE = 1;

export class TablesActionCell {
    static $inject = ['$element'];

    constructor($element) {
        Object.assign(this, {$element});
    }

    onClick(e) {
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
