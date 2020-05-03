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

const reportStartTime = report_topology["profilingStartTime"];
const reportFinishTime = report_topology["profilingFinishTime"];

$("#reportTiming").html("Collected from " + moment(reportStartTime).format() + " to "
    + moment(reportFinishTime).format());

Chart.plugins.register({
    afterDraw: function (chart) {
        if (chart.data.datasets.length === 0 || chart.data.datasets.every(val => val.data.length === 0)) {
            // No data is present
            var ctx = chart.chart.ctx;
            var width = chart.chart.width;
            var height = chart.chart.height
            //chart.clear()

            ctx.save();
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.font = "16px normal 'Helvetica Nueue'";
            ctx.fillText('No data to display', width / 2, height / 2);
            ctx.restore();
        }
    }
});

function sortByKeyDesc(array, key) {
    return array.sort(function (a, b) {
        var x = a[key];
        var y = b[key];

        return ((x > y) ? -1 : ((x < y) ? 1 : 0));
    });
}

function sortByKeyAsc(array, key) {
    return array.sort(function (a, b) {
        var x = a[key];
        var y = b[key];

        return ((x < y) ? -1 : ((x > y) ? 1 : 0));
    });
}

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function buildSelectCaches(el, callback) {
    el.append('<option data-content="<b>All caches</b>" value="total"/>');

    Object.keys(report_startedCaches).forEach(cacheId => report_startedCaches[cacheId].cacheId = cacheId);

    var cachesInfo = sortByKeyAsc(Object.values(report_startedCaches), "cacheName");

    console.log(cachesInfo);

    if (cachesInfo.length === 0)
        return;

    $.each(cachesInfo, function (k, info) {
        var system = '';

        if (!info.userCache)
            system = "<span class='badge badge-warning align-middle ml-2'>System</span>";

        el.append('<option data-content="' + info.cacheName + system + '" value="' + info.cacheId + '"/>');
    });

    el.on('changed.bs.select', function (e, clickedIndex, newValue, oldValue) {
        callback();
    });
}

function buildSelectNodes(el, callback) {
    el.append('<option data-content="<b>All nodes</b>" value="total"/>');

    var nodesInfo = report_topology["nodesInfo"];

    $.each(nodesInfo, function (nodeId, nodeInfo) {
        el.append('<option data-content="' + nodeId + '" value="' + nodeId + '"/>');
    });

    el.on('changed.bs.select', function (e, clickedIndex, newValue, oldValue) {
        callback();
    });
}
