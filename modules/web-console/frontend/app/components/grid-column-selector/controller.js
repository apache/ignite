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

import uniq from 'lodash/fp/uniq';
import difference from 'lodash/difference';
import findLast from 'lodash/findLast';

const hasGrouping = (changes) => {
    return changes.gridApi.currentValue !== changes.gridApi.previousValue && changes.gridApi.currentValue.grouping;
};
const id = (column) => column.categoryDisplayName || column.displayName || column.name;
const picksrc = (state) => state.categories.length ? state.categories : state.columns;

export default class GridColumnSelectorController {
    static $inject = ['$scope', 'uiGridConstants'];

    constructor($scope, uiGridConstants) {
        Object.assign(this, {$scope, uiGridConstants});
    }

    $onChanges(changes) {
        if (changes && 'gridApi' in changes && changes.gridApi.currentValue) {
            this.applyValues();
            this.gridApi.grid.registerDataChangeCallback(() => this.applyValues(), [this.uiGridConstants.dataChange.COLUMN]);
            if (hasGrouping(changes)) this.gridApi.grouping.on.groupingChanged(this.$scope, () => this.applyValues());
        }
    }

    applyValues() {
        this.state = this.getState();
        this.columnsMenu = this.makeMenu();
        this.selectedColumns = this.getSelectedColumns();
        this.setSelectedColumns();
    }

    getSelectedColumns() {
        return picksrc(this.state).filter((i) => i.isVisible && i.isInMenu).map((i) => id(i.item));
    }

    makeMenu() {
        return picksrc(this.state).filter((i) => i.isInMenu).map((i) => ({
            name: id(i.item),
            item: i.item
        }));
    }

    getState() {
        const api = this.gridApi;
        const columns = api.grid.options.columnDefs;
        const categories = api.grid.options.categories || [];
        const grouping = api.grouping
            ? api.grouping.getGrouping().grouping.map((g) => columns.find((c) => c.name === g.colName).categoryDisplayName)
            : [];
        const mapfn = (item) => ({
            item,
            isInMenu: item.enableHiding !== false && !grouping.includes(item.name),
            isVisible: grouping.includes(item.name) || item.visible !== false
        });
        return ({
            categories: categories.map(mapfn),
            columns: columns.map(mapfn)
        });
    }

    findScrollToNext(columns, prevColumns) {
        if (!prevColumns)
            return;

        const diff = difference(columns, prevColumns);

        if (diff.length === 1 && columns.includes(diff[0]))
            return diff[0];
    }

    setSelectedColumns() {
        const {selectedColumns} = this;
        const scrollToNext = this.findScrollToNext(selectedColumns, this.prevSelectedColumns);
        this.prevSelectedColumns = selectedColumns;
        const all = this.state.categories.concat(this.state.columns);
        const itemsToShow = uniq(all.filter((i) => !i.isInMenu && i.isVisible).map((i) => id(i.item)).concat(selectedColumns));
        (this.gridApi.grid.options.categories || []).concat(this.gridApi.grid.options.columnDefs).forEach((item) => {
            item.visible = itemsToShow.includes(id(item));
        });
        // Scrolls to the last matching columnDef, useful if it was out of view after being enabled.
        this.refreshColumns().then(() => {
            if (scrollToNext) {
                const column = findLast(this.gridApi.grid.options.columnDefs, (c) => id(c) === scrollToNext);
                this.gridApi.grid.scrollTo(null, column);
            }
        });
    }

    // gridApi.grid.refreshColumns method does not allow to wait until async operation completion.
    // This method does roughly the same, but returns a Promise.
    refreshColumns() {
        return this.gridApi.grid.processColumnsProcessors(this.gridApi.grid.columns)
        .then((renderableColumns) => this.gridApi.grid.setVisibleColumns(renderableColumns))
        .then(() => this.gridApi.grid.redrawInPlace())
        .then(() => this.gridApi.grid.refreshCanvas(true));
    }
}
