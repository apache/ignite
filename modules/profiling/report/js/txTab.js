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

function drawTxCharts() {
    var cacheId = searchCachesSelect.val();
    var nodeId = searchNodesSelect.val();

    txCharts.empty();

    $.each(TX_OPERATIONS, function (k, opName) {
        var txChartId = opName + "TxChart";

        txCharts.append('<canvas class="my-4" id="' + txChartId + '" height="120""></canvas>');

        new Chart(document.getElementById(txChartId), {
            type: 'line',
            data: {
                datasets: prepareTxDatasets(nodeId, cacheId, opName)
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
            labels: buildTxHistogramBuckets(),
            datasets: prepareTxHistogramDatasets(nodeId, cacheId)
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

function prepareTxHistogramDatasets(nodeId, cacheId) {
    var datasets = [];

    var datasetData = REPORT_DATA.txHistogram[nodeId] === undefined ? undefined : REPORT_DATA.txHistogram[nodeId][cacheId];

    if (datasetData === undefined)
        return datasets;

    var dataset = {
        label: 'Count of transactions',
        data: datasetData,
        backgroundColor: '#FAA586',
    }

    datasets.push(dataset);

    return datasets;
}

function prepareTxDatasets(nodeId, cacheId, opName) {
    var datasets = [];

    var txData = REPORT_DATA.tx[nodeId] === undefined ? undefined : REPORT_DATA.tx[nodeId][cacheId];

    if (txData === undefined)
        return datasets;

    var datasetData = [];

    $.each(txData[opName], function (time, arr) {
        datasetData.push({t: parseInt(arr[0]), y: arr[1]})
    });

    sortByKeyAsc(datasetData, "t");

    var dataset = {
        data: datasetData,
        label: "Count of " + opName,
        lineTension: 0,
        backgroundColor: 'transparent',
        borderWidth: 2,
        pointRadius: 1,
        borderColor: TX_COLORS[opName],
        pointBackgroundColor: TX_COLORS[opName]
    };

    datasets.push(dataset);

    return datasets;
}

function buildTxHistogramBuckets() {
    var buckets = [];

    var lastVal = 0;

    $.each(REPORT_DATA.txHistogramBuckets, function (idx, value) {
        buckets.push(lastVal + " to " + value + " ms");
        lastVal = value;
    });

    buckets.push(lastVal + " ms or more");

    return buckets;
}

buildSelectCaches(txSearchCachesSelect, drawTxCharts);
buildSelectNodes(txSearchNodesSelect, drawTxCharts);

drawTxCharts()
