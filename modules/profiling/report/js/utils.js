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

$("#reportTiming").html("Profiling started at " + moment(REPORT_DATA.clusterInfo.profilingStartTime).format());

/** Plugin for Charts JS to print 'No data to display' if there is no data for chart. */
Chart.plugins.register({
    afterDraw: function (chart) {
        if (chart.data.datasets.length === 0 || chart.data.datasets.every(val => val.data.length === 0)) {
            // No data is present
            var ctx = chart.chart.ctx;
            var width = chart.chart.width;
            var height = chart.chart.height

            ctx.save();
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.font = "16px normal 'Helvetica Nueue'";
            ctx.fillText('No data to display', width / 2, height / 2);
            ctx.restore();
        }
    }
});

/** Sorts array. */
function sortByKeyDesc(array, key) {
    return array.sort(function (a, b) {
        var x = a[key];
        var y = b[key];

        return ((x > y) ? -1 : ((x < y) ? 1 : 0));
    });
}

/** Sorts array. */
function sortByKeyAsc(array, key) {
    return array.sort(function (a, b) {
        var x = a[key];
        var y = b[key];

        return ((x < y) ? -1 : ((x > y) ? 1 : 0));
    });
}

/** Number with commas as thousands separators. */
function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/** Builds bootstrap-select for caches. */
function buildSelectCaches(el, onSelect) {
    el.append('<option data-content="<b>All caches</b>" value="total"/>');

    var caches = REPORT_DATA.clusterInfo.caches;

    Object.keys(caches).forEach(cacheId => caches[cacheId].cacheId = cacheId);

    caches = sortByKeyAsc(Object.values(caches), "cacheName");

    if (caches.length === 0)
        return;

    $.each(caches, function (idx, cache) {
        var system = '';

        if (!cache.userCache)
            system = "<span class='badge badge-warning align-middle ml-2'>System</span>";

        el.append('<option data-content="' + cache.cacheName + system + '" value="' + cache.cacheId + '"/>');
    });

    el.on('changed.bs.select', onSelect);
}

/** Builds bootstrap-select for nodes. */
function buildSelectNodes(el, onSelect) {
    el.append('<option data-content="<b>All nodes</b>" value="total"/>');

    var nodes = REPORT_DATA.clusterInfo.nodes;

    $.each(nodes, (nodeId, node) => {
        el.append('<option data-content="' + node.name + '" value="' + nodeId + '"/>');
    });

    el.on('changed.bs.select', onSelect);
}
