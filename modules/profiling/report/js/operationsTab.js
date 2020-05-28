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

const CACHE_OPERATIONS = ["GET", "PUT", "REMOVE", "GET_AND_PUT", "GET_AND_REMOVE",
    "GET_ALL", "PUT_ALL", "REMOVE_ALL", "INVOKE", "INVOKE_ALL", "LOCK"];

const CACHE_OPERATIONS_READABLE = ["get", "put", "remove", "getAndPut", "getAndRemove",
    "getAll", "putAll", "removeAll","invoke", "invokeAll", "lock"];

const CACHE_OPERATIONS_COLORS = {
    GET: "#007bff",
    PUT: "#4661EE",
    REMOVE: "#EC5657",
    GET_AND_PUT: "#c02332",
    GET_AND_REMOVE: "#9d71e4",
    GET_ALL: "#8357c7",
    PUT_ALL: "#1BCDD1",
    REMOVE_ALL: "#23BFAA",
    INVOKE: "#F5A52A",
    INVOKE_ALL: "#fd7e14",
    LOCK: "#FAA586"
};

const searchCachesSelect = $('#searchCaches');
const searchNodesSelect = $('#searchNodes');

var opsCountPerType = {};

function drawCacheCharts() {
    $("#operationsCharts").empty();

    $.each(CACHE_OPERATIONS, function (k, opName) {
        opsCountPerType[opName] = 0;

        var chartId = opName + "OperationChart";

        $("#operationsCharts").append('<canvas class="my-4" ' + 'id="' + chartId + '" height="120"/>');

        new Chart(document.getElementById(chartId), {
            type: 'line',
            data: {
                datasets: prepareCacheDatasets(opName)
            },
            options: {
                scales: {
                    xAxes: [{
                        type: 'time',
                        time: {
                            displayFormats: {
                                'millisecond': 'HH:mm:ss',
                                'second': 'HH:mm:ss',
                                'minute': 'HH:mm:ss',
                                'hour': 'HH:mm'
                            }
                        },
                        scaleLabel: {
                            display: true,
                            labelString: 'Date'
                        }
                    }],
                    yAxes: [{
                        display: true,
                        scaleLabel: {
                            display: true,
                            labelString: 'Count'
                        },
                        ticks: {
                            suggestedMin: 0,
                            suggestedMax: 10
                        }
                    }]
                },
                legend: {
                    display: true
                },
                title: {
                    display: true,
                    text: "Count of [" + CACHE_OPERATIONS_READABLE[k] + "]",
                    fontSize: 20
                },
                animation: false
            }
        })
    });

    drawCacheBar();
}

function prepareCacheDatasets(opName) {
    var cacheId = searchCachesSelect.val();
    var nodeId = searchNodesSelect.val();

    var datasets = [];

    var cacheOps = REPORT_DATA.cacheOps[nodeId] === undefined ? undefined : REPORT_DATA.cacheOps[nodeId][cacheId];

    if (cacheOps === undefined)
        return datasets;

    var datasetData = [];

    $.each(cacheOps[opName], function (k, arr) {
        datasetData.push({t: parseInt(arr[0]), y: arr[1]})

        opsCountPerType[opName] += arr[1];
    });

    sortByKeyAsc(datasetData, "t");

    var dataset = {
        data: datasetData,
        label: "Count of " + opName,
        lineTension: 0,
        backgroundColor: 'transparent',
        borderWidth: 2,
        pointRadius: 1,
        borderColor: CACHE_OPERATIONS_COLORS[opName],
        pointBackgroundColor: CACHE_OPERATIONS_COLORS[opName],
    };

    datasets.push(dataset);

    return datasets;
}

function drawCacheBar() {
    $("#operationsCharts").prepend('<canvas class="my-4" id="operationBarChart" height="60"/>');

    var data = [];
    var colors = [];

    $.each(CACHE_OPERATIONS, function (k, opName) {
        data[k] = opsCountPerType[opName];
        colors[k] = CACHE_OPERATIONS_COLORS[opName];
    });

    new Chart(document.getElementById("operationBarChart"), {
        type: 'bar',
        data: {
            datasets: [{
                data: data,
                backgroundColor: colors,
                label: 'Total count',
            }],
            labels: CACHE_OPERATIONS
        },
        options: {
            scales: {
                yAxes: [{
                    display: true,
                    scaleLabel: {
                        display: true,
                        labelString: 'Count'
                    },
                    ticks: {
                        suggestedMin: 0,
                        suggestedMax: 10
                    }
                }]
            },
            legend: {
                display: false
            },
            title: {
                display: true,
                text: 'Distribution of count operations by type',
                fontSize: 20
            },
            animation: false
        }
    });

}

buildSelectCaches(searchCachesSelect, drawCacheCharts);
buildSelectNodes(searchNodesSelect, drawCacheCharts);

drawCacheCharts()
