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

export default {
    template,
    controller: class {
        static $inject = ['$scope', 'uiGridGroupingConstants', 'uiGridExporterService', 'uiGridExporterConstants'];

        constructor($scope, uiGridGroupingConstants, uiGridExporterService, uiGridExporterConstants) {
            Object.assign(this, { uiGridGroupingConstants, uiGridExporterService, uiGridExporterConstants });
        }

        export() {
            const data = [];
            const columnHeaders = this.uiGridExporterService.getColumnHeaders(this.gridApi.grid, this.uiGridExporterConstants.VISIBLE);

            _.forEach(this.gridApi.grid.rows, (row) => {
                if (!row.visible)
                    return;

                const values = [];
                _.forEach(columnHeaders, ({ name }) => {
                    values.push({ value: row.entity[name] });
                });

                data.push(values);
            });

            const csvContent = this.uiGridExporterService.formatAsCsv(columnHeaders, data, this.gridApi.grid.options.exporterCsvColumnSeparator);
            this.uiGridExporterService.downloadFile(this.gridApi.grid.options.exporterCsvFilename, csvContent, this.gridApi.grid.options.exporterOlderExcelCompatibility);
        }
    },
    bindings: {
        gridApi: '<'
    }
};
