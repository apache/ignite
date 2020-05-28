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

/** Builds tasks statistics table. */
$('#computeStatisticsTable').bootstrapTable({
    pagination: true,
    search: true,
    columns: [{
        field: 'taskName',
        title: 'Task name',
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
        field: 'jobsCount',
        title: 'Jobs count',
        sortable: true
    }, {
        field: 'jobsTotalDuration',
        title: 'Jobs total duration',
        sortable: true
    }],
    data: prepareComputeTableData(),
    sortName: 'duration',
    sortOrder: 'desc'
})

/** Tasks statistics table data. */
function prepareComputeTableData() {
    var data = [];

    $.each(REPORT_DATA.totalCompute, function (taskName, taskData) {
        data.push({
            "taskName": taskName,
            "count": taskData["count"],
            "duration": taskData["duration"],
            "jobsCount": taskData["jobsCount"],
            "jobsTotalDuration": taskData["jobsTotalDuration"]
        });
    });

    return data;
}

/** Builds top slow task table. */
$('#topSlowComputeTable').bootstrapTable({
    pagination: true,
    search: true,
    columns: [{
        field: 'taskName',
        title: 'Task name',
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
        field: 'jobsTotalDuration',
        title: 'Jobs total duration',
        sortable: true
    }, {
        field: 'affPartId',
        title: 'Affinity partition id',
        sortable: true
    }],
    data: prepareSlowTasksTableData(),
    sortName: 'duration',
    sortOrder: 'desc',
    detailViewIcon: true,
    detailViewByClick: true,
    detailView: true,
    onExpandRow: function (index, row, $detail) {
        buildJobsSubTable($detail.html("<h5>Jobs</h5><table></table>").find('table'), row.index)
    }
})

/** Top of slow tasks table data. */
function prepareSlowTasksTableData() {
    var data = [];

    $.each(REPORT_DATA.topSlowCompute, function (k, sqlData) {
        data.push({
            taskName: sqlData["taskName"],
            duration: sqlData["duration"],
            startTime: new Date(sqlData["startTime"]),
            nodeId: sqlData["nodeId"],
            affPartId: sqlData["affPartId"],
            jobsTotalDuration: sqlData["jobsTotalDuration"],
            index: k
        });
    });

    return data;
}

/** Builds jobs statistics subtable. */
function buildJobsSubTable($el, index) {
    var jobs = REPORT_DATA.topSlowCompute[index]["jobs"];

    var data = [];

    $.each(jobs, function (k, job) {
        data.push({
            queuedTime: job["queuedTime"],
            duration: job["duration"],
            startTime: new Date(job["startTime"]),
            nodeId: job["nodeId"],
            isTimedOut: job["isTimedOut"]
        });
    });

    $el.bootstrapTable({
        columns: [{
            field: 'nodeId',
            title: 'Node id',
            sortable: true
        }, {
            field: 'queuedTime',
            title: 'Queued time, ms',
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
            field: 'isTimedOut',
            title: 'Timed out',
            sortable: true
        }],
        data: data,
        sortName: 'duration',
        sortOrder: 'desc'
    })
}
