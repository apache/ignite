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

export default class {
    static $inject = ['$scope', 'uiGridConstants'];

    constructor($scope, uiGridConstants) {
        Object.assign(this, {$scope, uiGridConstants});
    }

    $onChanges(changes) {
        if (changes && 'gridApi' in changes && changes.gridApi.currentValue && this.gridApi.selection) {
            this.applyValues();

            this.gridApi.grid.registerDataChangeCallback(() => this.applyValues(), [this.uiGridConstants.dataChange.ROW]);
            // Used to toggle selected of one row.
            this.gridApi.selection.on.rowSelectionChanged(this.$scope, () => this.applyValues());
            // Used to toggle all selected of rows.
            this.gridApi.selection.on.rowSelectionChangedBatch(this.$scope, () => this.applyValues());
        }
    }

    applyValues() {
        this.selected = this.gridApi.selection.legacyGetSelectedRows().length;
        this.count = this.gridApi.grid.rows.length;
    }
}
