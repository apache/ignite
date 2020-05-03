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

const TX_OPERATIONS = ["commit", "rollback"];

const TX_COLORS = {
    commit: "#1BCDD1",
    rollback: "#007bff"
};

const txSearchCachesSelect = $('#txSearchCaches');
const txSearchNodesSelect = $('#txSearchNodes');
const txCharts = $("#txCharts");

function tx_drawCharts() {
    var cacheId = searchCachesSelect.val();
    var nodeId = searchNodesSelect.val();

    txCharts.empty();

    $.each(TX_OPERATIONS, function (k, opName) {
        var txChartId = opName + "TxChart";

        txCharts.append('<canvas class="my-4" id="' + txChartId + '" height="120""></canvas>');

        new Chart(document.getElementById(txChartId), {
            type: 'line',
            data: {
                datasets: tx_prepareCacheDatasets(nodeId, cacheId, opName)
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
                    text: "Count of [" + opName + "]",
                    fontSize: 20
                },
                animation: false
            }
        })
    });

    txCharts.prepend('<canvas class="my-4" id="txHistogram" height="80""></canvas>');

    new Chart(document.getElementById("txHistogram"), {
        type: 'bar',
        data: {
            labels: buildHistogramBuckets(),
            datasets: tx_prepareHistogramDatasets(nodeId, cacheId)
        },
        options: {
            scales: {
                xAxes: [{
                    gridLines: {
                        offsetGridLines: true
                    },
                    scaleLabel: {
                        display: true,
                        labelString: 'Duration of transaction'
                    }
                }],
                yAxes: [{
                    display: true,
                    scaleLabel: {
                        display: true,
                        labelString: 'Count of transactions'
                    }
                }]
            },
            legend: {
                display: false
            },
            title: {
                display: true,
                text: "Histogram of transaction durations",
                fontSize: 20
            },
            animation: false
        }
    });
}

function tx_prepareHistogramDatasets(nodeId, cacheId) {
    var res = [];

    var data = report_txHistogram[nodeId] === undefined ? undefined : report_txHistogram[nodeId][cacheId];

    if (data === undefined)
        return res;

    var dataset = {
        label: 'Count of transactions',
        data: data,
        backgroundColor: '#FAA586',
    }

    res.push(dataset);

    return res;
}

function tx_prepareCacheDatasets(nodeId, cacheId, opName) {
    var res = [];

    var totalCacheStat = report_tx[nodeId] === undefined ? undefined : report_tx[nodeId][cacheId];

    if (totalCacheStat === undefined)
        return res;

    var data = [];

    $.each(totalCacheStat[opName], function (time, arr) {
        data.push({t: parseInt(arr[0]), y: arr[1]})
    });

    sortByKeyAsc(data, "t");

    var dataset = {
        data: data,
        label: "Count of " + opName,
        lineTension: 0,
        backgroundColor: 'transparent',
        borderWidth: 2,
        pointRadius: 1,
        borderColor: TX_COLORS[opName],
        pointBackgroundColor: TX_COLORS[opName]
    };

    res.push(dataset);

    return res;
}

function buildHistogramBuckets() {
    var buckets = [];

    var lastVal = 0;

    $.each(report_txHistogramBuckets, function (idx, value) {
        buckets.push(lastVal + " to " + value + " ms");
        lastVal = value;
    });

    buckets.push(lastVal + " ms or more");

    return buckets;
}

buildSelectCaches(txSearchCachesSelect, tx_drawCharts);
buildSelectNodes(txSearchNodesSelect, tx_drawCharts);

tx_drawCharts()
