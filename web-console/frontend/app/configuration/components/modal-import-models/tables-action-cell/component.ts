

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
