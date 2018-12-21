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
import {CSV} from 'app/services/CSV';

export default {
    template,
    controller: class {
        static $inject = ['$scope', 'uiGridGroupingConstants', 'uiGridExporterService', 'uiGridExporterConstants', 'CSV'];

        /**
         * @param {CSV} CSV
         */
        constructor($scope, uiGridGroupingConstants, uiGridExporterService, uiGridExporterConstants, CSV) {
            this.CSV = CSV;
            Object.assign(this, { uiGridGroupingConstants, uiGridExporterService, uiGridExporterConstants });
        }

        export() {
            const data = [];
            const grid = this.gridApi.grid;
            const exportColumnHeaders = this.uiGridExporterService.getColumnHeaders(grid, this.uiGridExporterConstants.VISIBLE);

            grid.rows.forEach((row) => {
                if (!row.visible)
                    return;

                const values = [];

                exportColumnHeaders.forEach((exportCol) => {
                    const col = grid.columns.find(({ field }) => field === exportCol.name);

                    if (!col || !col.visible || col.colDef.exporterSuppressExport === true)
                        return;

                    const value = grid.getCellValue(row, col);

                    values.push({ value });
                });

                data.push(values);
            });

            const csvContent = this.uiGridExporterService.formatAsCsv(exportColumnHeaders, data, this.CSV.getSeparator());

            const csvFileName = this.fileName || 'export.csv';

            this.uiGridExporterService.downloadFile(csvFileName, csvContent, this.gridApi.grid.options.exporterOlderExcelCompatibility);
        }
    },
    bindings: {
        gridApi: '<',
        fileName: '<'
    }
};
