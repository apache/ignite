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

$('#sqlStatisticsTable').bootstrapTable({
    pagination: true,
    search: true,
    columns: [{
        field: 'text',
        title: 'Query text',
        sortable: true
    }, {
        field: 'count',
        title: 'Executions',
        sortable: true
    }, {
        field: 'duration',
        title: 'Total duration, ms',
        sortable: true
    }, {
        field: 'logicalReads',
        title: 'Logical reads',
        sortable: true
    }, {
        field: 'physicalReads',
        title: 'Physical reads',
        sortable: true
    }, {
        field: 'failures',
        title: 'Failures count',
        sortable: true
    }],
    data: prepareSqlTableData(),
    sortName: 'duration',
    sortOrder: 'desc'
})

function prepareSqlTableData() {
    var data = [];

    $.each(REPORT_DATA.sql, function (queryText, sqlData) {
        data.push({
            "text": queryText,
            "count": sqlData["count"],
            "duration": sqlData["duration"],
            "logicalReads": sqlData["logicalReads"],
            "physicalReads": sqlData["physicalReads"],
            "failures": sqlData["failures"]
        });
    });

    return data;
}

$('#topSlowSqlTable').bootstrapTable({
    pagination: true,
    search: true,
    columns: [{
        field: 'text',
        title: 'Query text',
        sortable: true
    }, {
        field: 'duration',
        title: 'Duration, ms',
        sortable: true,
        sortOrder: 'desc'
    }, {
        field: 'startTime',
        title: 'Start time',
        sortable: true
    }, {
        field: 'nodeId',
        title: 'Originating node id',
        sortable: true
    }, {
        field: 'logicalReads',
        title: 'Logical reads',
        sortable: true
    }, {
        field: 'physicalReads',
        title: 'Physical reads',
        sortable: true
    }, {
        field: 'success',
        title: 'Success',
        sortable: true
    }],
    data: prepareSlowSqlTableData(),
    sortName: 'duration',
    sortOrder: 'desc'
})

function prepareSlowSqlTableData() {
    var data = [];

    $.each(REPORT_DATA.topSlowSql, function (key, sqlData) {
        data.push({
            text: sqlData["text"],
            duration: sqlData["duration"],
            startTime: new Date(sqlData["startTime"]),
            nodeId: sqlData["nodeId"],
            logicalReads: sqlData["logicalReads"],
            physicalReads: sqlData["physicalReads"],
            success: sqlData["success"]
        });
    });

    return data;
}
