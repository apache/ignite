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
import {nonEmpty, nonNil} from 'app/utils/lodashMixins';
import id8 from 'app/utils/id8';
import 'rxjs/add/operator/mergeMap';
import 'rxjs/add/operator/merge';
import 'rxjs/add/operator/switchMap';
import 'rxjs/add/operator/exhaustMap';
import 'rxjs/add/operator/distinctUntilChanged';

import { fromPromise } from 'rxjs/observable/fromPromise';
import { timer } from 'rxjs/observable/timer';
import { defer } from 'rxjs/observable/defer';

import {CSV} from 'app/services/CSV';

import paragraphRateTemplateUrl from 'views/sql/paragraph-rate.tpl.pug';
import cacheMetadataTemplateUrl from 'views/sql/cache-metadata.tpl.pug';
import chartSettingsTemplateUrl from 'views/sql/chart-settings.tpl.pug';
import messageTemplateUrl from 'views/templates/message.tpl.pug';

// Time line X axis descriptor.
const TIME_LINE = {value: -1, type: 'java.sql.Date', label: 'TIME_LINE'};

// Row index X axis descriptor.
const ROW_IDX = {value: -2, type: 'java.lang.Integer', label: 'ROW_IDX'};

const NON_COLLOCATED_JOINS_SINCE = '1.7.0';

const COLLOCATED_QUERY_SINCE = [['2.3.5', '2.4.0'], ['2.4.6', '2.5.0'], '2.5.2'];

const ENFORCE_JOIN_SINCE = [['1.7.9', '1.8.0'], ['1.8.4', '1.9.0'], '1.9.1'];

const LAZY_QUERY_SINCE = [['2.1.4-p1', '2.2.0'], '2.2.1'];

const DDL_SINCE = [['2.1.6', '2.2.0'], '2.3.0'];

const _fullColName = (col) => {
    const res = [];

    if (col.schemaName)
        res.push(col.schemaName);

    if (col.typeName)
        res.push(col.typeName);

    res.push(col.fieldName);

    return res.join('.');
};

let paragraphId = 0;

class Paragraph {
    constructor($animate, $timeout, JavaTypes, paragraph, errorParser) {
        const self = this;

        self.id = 'paragraph-' + paragraphId++;
        self.qryType = paragraph.qryType || 'query';
        self.maxPages = 0;
        self.filter = '';
        self.useAsDefaultSchema = false;
        self.localQueryMode = false;
        self.csvIsPreparing = false;
        self.scanningInProgress = false;

        _.assign(this, paragraph);

        Object.defineProperty(this, 'gridOptions', {value: {
            enableGridMenu: false,
            enableColumnMenus: false,
            flatEntityAccess: true,
            fastWatch: true,
            categories: [],
            rebuildColumns() {
                if (_.isNil(this.api))
                    return;

                this.categories.length = 0;

                this.columnDefs = _.reduce(self.meta, (cols, col, idx) => {
                    cols.push({
                        displayName: col.fieldName,
                        headerTooltip: _fullColName(col),
                        field: idx.toString(),
                        minWidth: 50,
                        cellClass: 'cell-left',
                        visible: self.columnFilter(col)
                    });

                    this.categories.push({
                        name: col.fieldName,
                        visible: self.columnFilter(col),
                        enableHiding: true
                    });

                    return cols;
                }, []);

                $timeout(() => this.api.core.notifyDataChange('column'));
            },
            adjustHeight() {
                if (_.isNil(this.api))
                    return;

                this.data = self.rows;

                const height = Math.min(self.rows.length, 15) * 30 + 47;

                // Remove header height.
                this.api.grid.element.css('height', height + 'px');

                $timeout(() => this.api.core.handleWindowResize());
            },
            onRegisterApi(api) {
                $animate.enabled(api.grid.element, false);

                this.api = api;

                this.rebuildColumns();

                this.adjustHeight();
            }
        }});

        Object.defineProperty(this, 'chartHistory', {value: []});

        Object.defineProperty(this, 'error', {value: {
            root: {},
            message: ''
        }});

        this.setError = (err) => {
            this.error.root = err;
            this.error.message = errorParser.extractMessage(err);

            let cause = err;

            while (nonNil(cause)) {
                if (nonEmpty(cause.className) &&
                    _.includes(['SQLException', 'JdbcSQLException', 'QueryCancelledException'], JavaTypes.shortClassName(cause.className))) {
                    this.error.message = errorParser.extractMessage(cause.message || cause.className);

                    break;
                }

                cause = cause.cause;
            }

            if (_.isEmpty(this.error.message) && nonEmpty(err.className)) {
                this.error.message = 'Internal cluster error';

                if (nonEmpty(err.className))
                    this.error.message += ': ' + err.className;
            }
        };
    }

    resultType() {
        if (_.isNil(this.queryArgs))
            return null;

        if (nonEmpty(this.error.message))
            return 'error';

        if (_.isEmpty(this.rows))
            return 'empty';

        return this.result === 'table' ? 'table' : 'chart';
    }

    nonRefresh() {
        return _.isNil(this.rate) || _.isNil(this.rate.stopTime);
    }

    table() {
        return this.result === 'table';
    }

    chart() {
        return this.result !== 'table' && this.result !== 'none';
    }

    nonEmpty() {
        return this.rows && this.rows.length > 0;
    }

    queryExecuted() {
        return nonEmpty(this.meta) || nonEmpty(this.error.message);
    }

    scanExplain() {
        return this.queryExecuted() && this.queryArgs.type !== 'QUERY';
    }

    timeLineSupported() {
        return this.result !== 'pie';
    }

    chartColumnsConfigured() {
        return nonEmpty(this.chartKeyCols) && nonEmpty(this.chartValCols);
    }

    chartTimeLineEnabled() {
        return nonEmpty(this.chartKeyCols) && _.eq(this.chartKeyCols[0], TIME_LINE);
    }

    executionInProgress(showLocal = false) {
        return this.loading && (this.localQueryMode === showLocal);
    }

    checkScanInProgress(showLocal = false) {
        return this.scanningInProgress && (this.localQueryMode === showLocal);
    }

    cancelRefresh($interval) {
        if (this.rate && this.rate.stopTime) {
            $interval.cancel(this.rate.stopTime);

            delete this.rate.stopTime;
        }
    }

    reset($interval) {
        this.meta = [];
        this.chartColumns = [];
        this.chartKeyCols = [];
        this.chartValCols = [];
        this.error.root = {};
        this.error.message = '';
        this.rows = [];
        this.duration = 0;

        this.cancelRefresh($interval);
    }
}

// Controller for SQL notebook screen.
export class NotebookCtrl {
    static $inject = ['$rootScope', '$scope', '$http', '$q', '$timeout', '$interval', '$animate', '$location', '$anchorScroll', '$state', '$filter', '$modal', '$popover', 'IgniteLoading', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteConfirm', 'AgentManager', 'IgniteChartColors', 'IgniteNotebook', 'IgniteNodes', 'uiGridExporterConstants', 'IgniteVersion', 'IgniteActivitiesData', 'JavaTypes', 'IgniteCopyToClipboard', CSV.name, 'IgniteErrorParser'];

    /**
     * @param {CSV} CSV
     */
    constructor($root, $scope, $http, $q, $timeout, $interval, $animate, $location, $anchorScroll, $state, $filter, $modal, $popover, Loading, LegacyUtils, Messages, Confirm, agentMgr, IgniteChartColors, Notebook, Nodes, uiGridExporterConstants, Version, ActivitiesData, JavaTypes, IgniteCopyToClipboard, CSV, errorParser) {
        const $ctrl = this;

        this.CSV = CSV;
        Object.assign(this, { $root, $scope, $http, $q, $timeout, $interval, $animate, $location, $anchorScroll, $state, $filter, $modal, $popover, Loading, LegacyUtils, Messages, Confirm, agentMgr, IgniteChartColors, Notebook, Nodes, uiGridExporterConstants, Version, ActivitiesData, JavaTypes, errorParser });

        // Define template urls.
        $ctrl.paragraphRateTemplateUrl = paragraphRateTemplateUrl;
        $ctrl.cacheMetadataTemplateUrl = cacheMetadataTemplateUrl;
        $ctrl.chartSettingsTemplateUrl = chartSettingsTemplateUrl;
        $ctrl.demoStarted = false;

        this.isDemo = $root.IgniteDemoMode;

        const _tryStopRefresh = function(paragraph) {
            paragraph.cancelRefresh($interval);
        };

        const _stopTopologyRefresh = () => {
            if ($scope.notebook && $scope.notebook.paragraphs)
                $scope.notebook.paragraphs.forEach((paragraph) => _tryStopRefresh(paragraph));
        };

        $scope.$on('$stateChangeStart', _stopTopologyRefresh);

        $scope.caches = [];

        $scope.pageSizes = [50, 100, 200, 400, 800, 1000];
        $scope.maxPages = [
            {label: 'Unlimited', value: 0},
            {label: '1', value: 1},
            {label: '5', value: 5},
            {label: '10', value: 10},
            {label: '20', value: 20},
            {label: '50', value: 50},
            {label: '100', value: 100}
        ];

        $scope.timeLineSpans = ['1', '5', '10', '15', '30'];

        $scope.aggregateFxs = ['FIRST', 'LAST', 'MIN', 'MAX', 'SUM', 'AVG', 'COUNT'];

        $scope.modes = LegacyUtils.mkOptions(['PARTITIONED', 'REPLICATED', 'LOCAL']);

        $scope.loadingText = $root.IgniteDemoMode ? 'Demo grid is starting. Please wait...' : 'Loading query notebook screen...';

        $scope.timeUnit = [
            {value: 1000, label: 'seconds', short: 's'},
            {value: 60000, label: 'minutes', short: 'm'},
            {value: 3600000, label: 'hours', short: 'h'}
        ];

        $scope.metadata = [];

        $scope.metaFilter = '';

        $scope.metaOptions = {
            nodeChildren: 'children',
            dirSelectable: true,
            injectClasses: {
                iExpanded: 'fa fa-minus-square-o',
                iCollapsed: 'fa fa-plus-square-o'
            }
        };

        const maskCacheName = $filter('defaultName');

        // We need max 1800 items to hold history for 30 mins in case of refresh every second.
        const HISTORY_LENGTH = 1800;

        const MAX_VAL_COLS = IgniteChartColors.length;

        $anchorScroll.yOffset = 55;

        $scope.chartColor = function(index) {
            return {color: 'white', 'background-color': IgniteChartColors[index]};
        };

        function _chartNumber(arr, idx, dflt) {
            if (idx >= 0 && arr && arr.length > idx && _.isNumber(arr[idx]))
                return arr[idx];

            return dflt;
        }

        function _min(rows, idx, dflt) {
            let min = _chartNumber(rows[0], idx, dflt);

            _.forEach(rows, (row) => {
                const v = _chartNumber(row, idx, dflt);

                if (v < min)
                    min = v;
            });

            return min;
        }

        function _max(rows, idx, dflt) {
            let max = _chartNumber(rows[0], idx, dflt);

            _.forEach(rows, (row) => {
                const v = _chartNumber(row, idx, dflt);

                if (v > max)
                    max = v;
            });

            return max;
        }

        function _sum(rows, idx) {
            let sum = 0;

            _.forEach(rows, (row) => sum += _chartNumber(row, idx, 0));

            return sum;
        }

        function _aggregate(rows, aggFx, idx, dflt) {
            const len = rows.length;

            switch (aggFx) {
                case 'FIRST':
                    return _chartNumber(rows[0], idx, dflt);

                case 'LAST':
                    return _chartNumber(rows[len - 1], idx, dflt);

                case 'MIN':
                    return _min(rows, idx, dflt);

                case 'MAX':
                    return _max(rows, idx, dflt);

                case 'SUM':
                    return _sum(rows, idx);

                case 'AVG':
                    return len > 0 ? _sum(rows, idx) / len : 0;

                case 'COUNT':
                    return len;

                default:
            }

            return 0;
        }

        function _chartLabel(arr, idx, dflt) {
            if (arr && arr.length > idx && _.isString(arr[idx]))
                return arr[idx];

            return dflt;
        }

        function _chartDatum(paragraph) {
            let datum = [];

            if (paragraph.chartColumnsConfigured()) {
                paragraph.chartValCols.forEach(function(valCol) {
                    let index = 0;
                    let values = [];
                    const colIdx = valCol.value;

                    if (paragraph.chartTimeLineEnabled()) {
                        const aggFx = valCol.aggFx;
                        const colLbl = valCol.label + ' [' + aggFx + ']';

                        if (paragraph.charts && paragraph.charts.length === 1)
                            datum = paragraph.charts[0].data;

                        const chartData = _.find(datum, {series: valCol.label});

                        const leftBound = new Date();
                        leftBound.setMinutes(leftBound.getMinutes() - parseInt(paragraph.timeLineSpan, 10));

                        if (chartData) {
                            const lastItem = _.last(paragraph.chartHistory);

                            values = chartData.values;

                            values.push({
                                x: lastItem.tm,
                                y: _aggregate(lastItem.rows, aggFx, colIdx, index++)
                            });

                            while (values.length > 0 && values[0].x < leftBound)
                                values.shift();
                        }
                        else {
                            _.forEach(paragraph.chartHistory, (history) => {
                                if (history.tm >= leftBound) {
                                    values.push({
                                        x: history.tm,
                                        y: _aggregate(history.rows, aggFx, colIdx, index++)
                                    });
                                }
                            });

                            datum.push({series: valCol.label, key: colLbl, values});
                        }
                    }
                    else {
                        index = paragraph.total;

                        values = _.map(paragraph.rows, function(row) {
                            const xCol = paragraph.chartKeyCols[0].value;

                            const v = {
                                x: _chartNumber(row, xCol, index),
                                xLbl: _chartLabel(row, xCol, null),
                                y: _chartNumber(row, colIdx, index)
                            };

                            index++;

                            return v;
                        });

                        datum.push({series: valCol.label, key: valCol.label, values});
                    }
                });
            }

            return datum;
        }

        function _xX(d) {
            return d.x;
        }

        function _yY(d) {
            return d.y;
        }

        function _xAxisTimeFormat(d) {
            return d3.time.format('%X')(new Date(d));
        }

        const _intClasses = ['java.lang.Byte', 'java.lang.Integer', 'java.lang.Long', 'java.lang.Short'];

        function _intType(cls) {
            return _.includes(_intClasses, cls);
        }

        const _xAxisWithLabelFormat = function(paragraph) {
            return function(d) {
                const values = paragraph.charts[0].data[0].values;

                const fmt = _intType(paragraph.chartKeyCols[0].type) ? 'd' : ',.2f';

                const dx = values[d];

                if (!dx)
                    return d3.format(fmt)(d);

                const lbl = dx.xLbl;

                return lbl ? lbl : d3.format(fmt)(d);
            };
        };

        function _xAxisLabel(paragraph) {
            return _.isEmpty(paragraph.chartKeyCols) ? 'X' : paragraph.chartKeyCols[0].label;
        }

        const _yAxisFormat = function(d) {
            const fmt = d < 1000 ? ',.2f' : '.3s';

            return d3.format(fmt)(d);
        };

        function _updateCharts(paragraph) {
            $timeout(() => _.forEach(paragraph.charts, (chart) => chart.api.update()), 100);
        }

        function _updateChartsWithData(paragraph, newDatum) {
            $timeout(() => {
                if (!paragraph.chartTimeLineEnabled()) {
                    const chartDatum = paragraph.charts[0].data;

                    chartDatum.length = 0;

                    _.forEach(newDatum, (series) => chartDatum.push(series));
                }

                paragraph.charts[0].api.update();
            });
        }

        function _yAxisLabel(paragraph) {
            const cols = paragraph.chartValCols;

            const tml = paragraph.chartTimeLineEnabled();

            return _.isEmpty(cols) ? 'Y' : _.map(cols, function(col) {
                let lbl = col.label;

                if (tml)
                    lbl += ' [' + col.aggFx + ']';

                return lbl;
            }).join(', ');
        }

        function _barChart(paragraph) {
            const datum = _chartDatum(paragraph);

            if (_.isEmpty(paragraph.charts)) {
                const stacked = paragraph.chartsOptions && paragraph.chartsOptions.barChart
                    ? paragraph.chartsOptions.barChart.stacked
                    : true;

                const options = {
                    chart: {
                        type: 'multiBarChart',
                        height: 400,
                        margin: {left: 70},
                        duration: 0,
                        x: _xX,
                        y: _yY,
                        xAxis: {
                            axisLabel: _xAxisLabel(paragraph),
                            tickFormat: paragraph.chartTimeLineEnabled() ? _xAxisTimeFormat : _xAxisWithLabelFormat(paragraph),
                            showMaxMin: false
                        },
                        yAxis: {
                            axisLabel: _yAxisLabel(paragraph),
                            tickFormat: _yAxisFormat
                        },
                        color: IgniteChartColors,
                        stacked,
                        showControls: true,
                        legend: {
                            vers: 'furious',
                            margin: {right: -15}
                        }
                    }
                };

                paragraph.charts = [{options, data: datum}];

                _updateCharts(paragraph);
            }
            else
                _updateChartsWithData(paragraph, datum);
        }

        function _pieChartDatum(paragraph) {
            const datum = [];

            if (paragraph.chartColumnsConfigured() && !paragraph.chartTimeLineEnabled()) {
                paragraph.chartValCols.forEach(function(valCol) {
                    let index = paragraph.total;

                    const values = _.map(paragraph.rows, (row) => {
                        const xCol = paragraph.chartKeyCols[0].value;

                        const v = {
                            x: xCol < 0 ? index : row[xCol],
                            y: _chartNumber(row, valCol.value, index)
                        };

                        // Workaround for known problem with zero values on Pie chart.
                        if (v.y === 0)
                            v.y = 0.0001;

                        index++;

                        return v;
                    });

                    datum.push({series: paragraph.chartKeyCols[0].label, key: valCol.label, values});
                });
            }

            return datum;
        }

        function _pieChart(paragraph) {
            let datum = _pieChartDatum(paragraph);

            if (datum.length === 0)
                datum = [{values: []}];

            paragraph.charts = _.map(datum, function(data) {
                return {
                    options: {
                        chart: {
                            type: 'pieChart',
                            height: 400,
                            duration: 0,
                            x: _xX,
                            y: _yY,
                            showLabels: true,
                            labelThreshold: 0.05,
                            labelType: 'percent',
                            donut: true,
                            donutRatio: 0.35,
                            legend: {
                                vers: 'furious',
                                margin: {right: -15}
                            }
                        },
                        title: {
                            enable: true,
                            text: data.key
                        }
                    },
                    data: data.values
                };
            });

            _updateCharts(paragraph);
        }

        function _lineChart(paragraph) {
            const datum = _chartDatum(paragraph);

            if (_.isEmpty(paragraph.charts)) {
                const options = {
                    chart: {
                        type: 'lineChart',
                        height: 400,
                        margin: { left: 70 },
                        duration: 0,
                        x: _xX,
                        y: _yY,
                        xAxis: {
                            axisLabel: _xAxisLabel(paragraph),
                            tickFormat: paragraph.chartTimeLineEnabled() ? _xAxisTimeFormat : _xAxisWithLabelFormat(paragraph),
                            showMaxMin: false
                        },
                        yAxis: {
                            axisLabel: _yAxisLabel(paragraph),
                            tickFormat: _yAxisFormat
                        },
                        color: IgniteChartColors,
                        useInteractiveGuideline: true,
                        legend: {
                            vers: 'furious',
                            margin: {right: -15}
                        }
                    }
                };

                paragraph.charts = [{options, data: datum}];

                _updateCharts(paragraph);
            }
            else
                _updateChartsWithData(paragraph, datum);
        }

        function _areaChart(paragraph) {
            const datum = _chartDatum(paragraph);

            if (_.isEmpty(paragraph.charts)) {
                const style = paragraph.chartsOptions && paragraph.chartsOptions.areaChart
                    ? paragraph.chartsOptions.areaChart.style
                    : 'stack';

                const options = {
                    chart: {
                        type: 'stackedAreaChart',
                        height: 400,
                        margin: {left: 70},
                        duration: 0,
                        x: _xX,
                        y: _yY,
                        xAxis: {
                            axisLabel: _xAxisLabel(paragraph),
                            tickFormat: paragraph.chartTimeLineEnabled() ? _xAxisTimeFormat : _xAxisWithLabelFormat(paragraph),
                            showMaxMin: false
                        },
                        yAxis: {
                            axisLabel: _yAxisLabel(paragraph),
                            tickFormat: _yAxisFormat
                        },
                        color: IgniteChartColors,
                        style,
                        legend: {
                            vers: 'furious',
                            margin: {right: -15}
                        }
                    }
                };

                paragraph.charts = [{options, data: datum}];

                _updateCharts(paragraph);
            }
            else
                _updateChartsWithData(paragraph, datum);
        }

        function _chartApplySettings(paragraph, resetCharts) {
            if (resetCharts)
                paragraph.charts = [];

            if (paragraph.chart() && paragraph.nonEmpty()) {
                switch (paragraph.result) {
                    case 'bar':
                        _barChart(paragraph);
                        break;

                    case 'pie':
                        _pieChart(paragraph);
                        break;

                    case 'line':
                        _lineChart(paragraph);
                        break;

                    case 'area':
                        _areaChart(paragraph);
                        break;

                    default:
                }
            }
        }

        $scope.chartRemoveKeyColumn = function(paragraph, index) {
            paragraph.chartKeyCols.splice(index, 1);

            _chartApplySettings(paragraph, true);
        };

        $scope.chartRemoveValColumn = function(paragraph, index) {
            paragraph.chartValCols.splice(index, 1);

            _chartApplySettings(paragraph, true);
        };

        $scope.chartAcceptKeyColumn = function(paragraph, item) {
            const accepted = _.findIndex(paragraph.chartKeyCols, item) < 0;

            if (accepted) {
                paragraph.chartKeyCols = [item];

                _chartApplySettings(paragraph, true);
            }

            return false;
        };

        const _numberClasses = ['java.math.BigDecimal', 'java.lang.Byte', 'java.lang.Double',
            'java.lang.Float', 'java.lang.Integer', 'java.lang.Long', 'java.lang.Short'];

        const _numberType = function(cls) {
            return _.includes(_numberClasses, cls);
        };

        $scope.chartAcceptValColumn = function(paragraph, item) {
            const valCols = paragraph.chartValCols;

            const accepted = _.findIndex(valCols, item) < 0 && item.value >= 0 && _numberType(item.type);

            if (accepted) {
                if (valCols.length === MAX_VAL_COLS - 1)
                    valCols.shift();

                valCols.push(item);

                _chartApplySettings(paragraph, true);
            }

            return false;
        };

        $scope.scrollParagraphs = [];

        $scope.rebuildScrollParagraphs = function() {
            $scope.scrollParagraphs = $scope.notebook.paragraphs.map(function(paragraph) {
                return {
                    text: paragraph.name,
                    click: 'scrollToParagraph("' + paragraph.id + '")'
                };
            });
        };

        $scope.scrollToParagraph = (id) => {
            const idx = _.findIndex($scope.notebook.paragraphs, {id});

            if (idx >= 0) {
                if (!_.includes($scope.notebook.expandedParagraphs, idx))
                    $scope.notebook.expandedParagraphs = $scope.notebook.expandedParagraphs.concat([idx]);

                if ($scope.notebook.paragraphs[idx].ace)
                    setTimeout(() => $scope.notebook.paragraphs[idx].ace.focus());
            }

            $location.hash(id);

            $anchorScroll();
        };

        const _hideColumn = (col) => col.fieldName !== '_KEY' && col.fieldName !== '_VAL';

        const _allColumn = () => true;

        $scope.aceInit = function(paragraph) {
            return function(editor) {
                editor.setAutoScrollEditorIntoView(true);
                editor.$blockScrolling = Infinity;

                const renderer = editor.renderer;

                renderer.setHighlightGutterLine(false);
                renderer.setShowPrintMargin(false);
                renderer.setOption('fontFamily', 'monospace');
                renderer.setOption('fontSize', '14px');
                renderer.setOption('minLines', '5');
                renderer.setOption('maxLines', '15');

                editor.setTheme('ace/theme/chrome');

                Object.defineProperty(paragraph, 'ace', { value: editor });
            };
        };

        /**
         * Update caches list.
         */
        const _refreshFn = () => {
            return agentMgr.topology(true)
                .then((nodes) => {
                    $scope.caches = _.sortBy(_.reduce(nodes, (cachesAcc, node) => {
                        _.forEach(node.caches, (cache) => {
                            let item = _.find(cachesAcc, {name: cache.name});

                            if (_.isNil(item)) {
                                cache.label = maskCacheName(cache.name, true);
                                cache.value = cache.name;

                                cache.nodes = [];

                                cachesAcc.push(item = cache);
                            }

                            item.nodes.push({
                                nid: node.nodeId.toUpperCase(),
                                ip: _.head(node.attributes['org.apache.ignite.ips'].split(', ')),
                                version: node.attributes['org.apache.ignite.build.ver'],
                                gridName: node.attributes['org.apache.ignite.ignite.name'],
                                os: `${node.attributes['os.name']} ${node.attributes['os.arch']} ${node.attributes['os.version']}`,
                                client: node.attributes['org.apache.ignite.cache.client']
                            });
                        });

                        return cachesAcc;
                    }, []), (cache) => cache.label.toLowerCase());

                    // Reset to first cache in case of stopped selected.
                    const cacheNames = _.map($scope.caches, (cache) => cache.value);

                    _.forEach($scope.notebook.paragraphs, (paragraph) => {
                        if (!_.includes(cacheNames, paragraph.cacheName))
                            paragraph.cacheName = _.head(cacheNames);
                    });

                    // Await for demo caches.
                    if (!$ctrl.demoStarted && $root.IgniteDemoMode && nonEmpty(cacheNames)) {
                        $ctrl.demoStarted = true;

                        Loading.finish('sqlLoading');

                        _.forEach($scope.notebook.paragraphs, (paragraph) => $scope.execute(paragraph));
                    }
                })
                .catch((err) => Messages.showError(err));
        };

        const _startWatch = () => {
            const awaitClusters$ = fromPromise(
                agentMgr.startClusterWatch('Leave Queries', 'default-state'));

            const finishLoading$ = defer(() => {
                if (!$root.IgniteDemoMode)
                    Loading.finish('sqlLoading');
            }).take(1);

            const refreshCaches = (period) => {
                return timer(0, period).exhaustMap(() => _refreshFn()).merge(finishLoading$);
            };

            this.refresh$ = awaitClusters$
                .mergeMap(() => agentMgr.currentCluster$)
                .do(() => Loading.start('sqlLoading'))
                .do(() => {
                    _.forEach($scope.notebook.paragraphs, (paragraph) => {
                        paragraph.reset($interval);
                    });
                })
                .switchMap(() => refreshCaches(5000))
                .subscribe();
        };

        Notebook.find($state.params.noteId)
            .then((notebook) => {
                $scope.notebook = _.cloneDeep(notebook);

                $scope.notebook_name = $scope.notebook.name;

                if (!$scope.notebook.expandedParagraphs)
                    $scope.notebook.expandedParagraphs = [];

                if (!$scope.notebook.paragraphs)
                    $scope.notebook.paragraphs = [];

                $scope.notebook.paragraphs = _.map($scope.notebook.paragraphs,
                    (paragraph) => new Paragraph($animate, $timeout, JavaTypes, paragraph, this.errorParser));

                if (_.isEmpty($scope.notebook.paragraphs))
                    $scope.addQuery();
                else
                    $scope.rebuildScrollParagraphs();
            })
            .then(() => _startWatch())
            .catch(() => {
                $scope.notebookLoadFailed = true;

                Loading.finish('sqlLoading');
            });

        $scope.renameNotebook = (name) => {
            if (!name)
                return;

            if ($scope.notebook.name !== name) {
                const prevName = $scope.notebook.name;

                $scope.notebook.name = name;

                Notebook.save($scope.notebook)
                    .then(() => $scope.notebook.edit = false)
                    .catch((err) => {
                        $scope.notebook.name = prevName;

                        Messages.showError(err);
                    });
            }
            else
                $scope.notebook.edit = false;
        };

        $scope.removeNotebook = (notebook) => Notebook.remove(notebook);

        $scope.renameParagraph = function(paragraph, newName) {
            if (!newName)
                return;

            if (paragraph.name !== newName) {
                paragraph.name = newName;

                $scope.rebuildScrollParagraphs();

                Notebook.save($scope.notebook)
                    .then(() => paragraph.edit = false)
                    .catch(Messages.showError);
            }
            else
                paragraph.edit = false;
        };

        $scope.addParagraph = (paragraph, sz) => {
            if ($scope.caches && $scope.caches.length > 0)
                paragraph.cacheName = _.head($scope.caches).value;

            $scope.notebook.paragraphs.push(paragraph);

            $scope.notebook.expandedParagraphs.push(sz);

            $scope.rebuildScrollParagraphs();

            $location.hash(paragraph.id);
        };

        $scope.addQuery = function() {
            const sz = $scope.notebook.paragraphs.length;

            ActivitiesData.post({ action: '/queries/add/query' });

            const paragraph = new Paragraph($animate, $timeout, JavaTypes, {
                name: 'Query' + (sz === 0 ? '' : sz),
                query: '',
                pageSize: $scope.pageSizes[1],
                timeLineSpan: $scope.timeLineSpans[0],
                result: 'none',
                rate: {
                    value: 1,
                    unit: 60000,
                    installed: false
                },
                qryType: 'query',
                lazy: true
            });

            $scope.addParagraph(paragraph, sz);

            $timeout(() => {
                $anchorScroll();

                paragraph.ace.focus();
            });
        };

        $scope.addScan = function() {
            const sz = $scope.notebook.paragraphs.length;

            ActivitiesData.post({ action: '/queries/add/scan' });

            const paragraph = new Paragraph($animate, $timeout, JavaTypes, {
                name: 'Scan' + (sz === 0 ? '' : sz),
                query: '',
                pageSize: $scope.pageSizes[1],
                timeLineSpan: $scope.timeLineSpans[0],
                result: 'none',
                rate: {
                    value: 1,
                    unit: 60000,
                    installed: false
                },
                qryType: 'scan'
            });

            $scope.addParagraph(paragraph, sz);
        };

        function _saveChartSettings(paragraph) {
            if (!_.isEmpty(paragraph.charts)) {
                const chart = paragraph.charts[0].api.getScope().chart;

                if (!LegacyUtils.isDefined(paragraph.chartsOptions))
                    paragraph.chartsOptions = {barChart: {stacked: true}, areaChart: {style: 'stack'}};

                switch (paragraph.result) {
                    case 'bar':
                        paragraph.chartsOptions.barChart.stacked = chart.stacked();

                        break;

                    case 'area':
                        paragraph.chartsOptions.areaChart.style = chart.style();

                        break;

                    default:
                }
            }
        }

        $scope.setResult = function(paragraph, new_result) {
            if (paragraph.result === new_result)
                return;

            _saveChartSettings(paragraph);

            paragraph.result = new_result;

            if (paragraph.chart())
                _chartApplySettings(paragraph, true);
        };

        $scope.resultEq = function(paragraph, result) {
            return (paragraph.result === result);
        };

        $scope.removeParagraph = function(paragraph) {
            Confirm.confirm('Are you sure you want to remove query: "' + paragraph.name + '"?')
                .then(function() {
                    $scope.stopRefresh(paragraph);

                    const paragraph_idx = _.findIndex($scope.notebook.paragraphs, function(item) {
                        return paragraph === item;
                    });

                    const panel_idx = _.findIndex($scope.expandedParagraphs, function(item) {
                        return paragraph_idx === item;
                    });

                    if (panel_idx >= 0)
                        $scope.expandedParagraphs.splice(panel_idx, 1);

                    $scope.notebook.paragraphs.splice(paragraph_idx, 1);

                    $scope.rebuildScrollParagraphs();

                    Notebook.save($scope.notebook)
                        .catch(Messages.showError);
                });
        };

        $scope.paragraphExpanded = function(paragraph) {
            const paragraph_idx = _.findIndex($scope.notebook.paragraphs, function(item) {
                return paragraph === item;
            });

            const panel_idx = _.findIndex($scope.notebook.expandedParagraphs, function(item) {
                return paragraph_idx === item;
            });

            return panel_idx >= 0;
        };

        const _columnFilter = function(paragraph) {
            return paragraph.disabledSystemColumns || paragraph.systemColumns ? _allColumn : _hideColumn;
        };

        const _notObjectType = function(cls) {
            return LegacyUtils.isJavaBuiltInClass(cls);
        };

        function _retainColumns(allCols, curCols, acceptableType, xAxis, unwantedCols) {
            const retainedCols = [];

            const availableCols = xAxis ? allCols : _.filter(allCols, function(col) {
                return col.value >= 0;
            });

            if (availableCols.length > 0) {
                curCols.forEach(function(curCol) {
                    const col = _.find(availableCols, {label: curCol.label});

                    if (col && acceptableType(col.type)) {
                        col.aggFx = curCol.aggFx;

                        retainedCols.push(col);
                    }
                });

                // If nothing was restored, add first acceptable column.
                if (_.isEmpty(retainedCols)) {
                    let col;

                    if (unwantedCols)
                        col = _.find(availableCols, (avCol) => !_.find(unwantedCols, {label: avCol.label}) && acceptableType(avCol.type));

                    if (!col)
                        col = _.find(availableCols, (avCol) => acceptableType(avCol.type));

                    if (col)
                        retainedCols.push(col);
                }
            }

            return retainedCols;
        }

        const _rebuildColumns = function(paragraph) {
            _.forEach(_.groupBy(paragraph.meta, 'fieldName'), function(colsByName, fieldName) {
                const colsByTypes = _.groupBy(colsByName, 'typeName');

                const needType = _.keys(colsByTypes).length > 1;

                _.forEach(colsByTypes, function(colsByType, typeName) {
                    _.forEach(colsByType, function(col, ix) {
                        col.fieldName = (needType && !LegacyUtils.isEmptyString(typeName) ? typeName + '.' : '') + fieldName + (ix > 0 ? ix : '');
                    });
                });
            });

            paragraph.gridOptions.rebuildColumns();

            paragraph.chartColumns = _.reduce(paragraph.meta, (acc, col, idx) => {
                if (_notObjectType(col.fieldTypeName)) {
                    acc.push({
                        label: col.fieldName,
                        type: col.fieldTypeName,
                        aggFx: $scope.aggregateFxs[0],
                        value: idx.toString()
                    });
                }

                return acc;
            }, []);

            if (paragraph.chartColumns.length > 0) {
                paragraph.chartColumns.push(TIME_LINE);
                paragraph.chartColumns.push(ROW_IDX);
            }

            // We could accept onl not object columns for X axis.
            paragraph.chartKeyCols = _retainColumns(paragraph.chartColumns, paragraph.chartKeyCols, _notObjectType, true);

            // We could accept only numeric columns for Y axis.
            paragraph.chartValCols = _retainColumns(paragraph.chartColumns, paragraph.chartValCols, _numberType, false, paragraph.chartKeyCols);
        };

        $scope.toggleSystemColumns = function(paragraph) {
            if (paragraph.disabledSystemColumns)
                return;

            paragraph.columnFilter = _columnFilter(paragraph);

            paragraph.chartColumns = [];

            _rebuildColumns(paragraph);
        };

        const _showLoading = (paragraph, enable) => paragraph.loading = enable;

        /**
         * @param {Object} paragraph Query
         * @param {Boolean} clearChart Flag is need clear chart model.
         * @param {{columns: Array, rows: Array, responseNodeId: String, queryId: int, hasMore: Boolean}} res Query results.
         * @private
         */
        const _processQueryResult = (paragraph, clearChart, res) => {
            const prevKeyCols = paragraph.chartKeyCols;
            const prevValCols = paragraph.chartValCols;

            if (!_.eq(paragraph.meta, res.columns)) {
                paragraph.meta = [];

                paragraph.chartColumns = [];

                if (!LegacyUtils.isDefined(paragraph.chartKeyCols))
                    paragraph.chartKeyCols = [];

                if (!LegacyUtils.isDefined(paragraph.chartValCols))
                    paragraph.chartValCols = [];

                if (res.columns.length) {
                    const _key = _.find(res.columns, {fieldName: '_KEY'});
                    const _val = _.find(res.columns, {fieldName: '_VAL'});

                    paragraph.disabledSystemColumns = !(_key && _val) ||
                        (res.columns.length === 2 && _key && _val) ||
                        (res.columns.length === 1 && (_key || _val));
                }

                paragraph.columnFilter = _columnFilter(paragraph);

                paragraph.meta = res.columns;

                _rebuildColumns(paragraph);
            }

            paragraph.page = 1;

            paragraph.total = 0;

            paragraph.duration = res.duration;

            paragraph.queryId = res.hasMore ? res.queryId : null;

            paragraph.resNodeId = res.responseNodeId;

            paragraph.setError({message: ''});

            // Prepare explain results for display in table.
            if (paragraph.queryArgs.query && paragraph.queryArgs.query.startsWith('EXPLAIN') && res.rows) {
                paragraph.rows = [];

                res.rows.forEach((row, i) => {
                    const line = res.rows.length - 1 === i ? row[0] : row[0] + '\n';

                    line.replace(/\"/g, '').split('\n').forEach((ln) => paragraph.rows.push([ln]));
                });
            }
            else
                paragraph.rows = res.rows;

            paragraph.gridOptions.adjustHeight(paragraph.rows.length);

            const chartHistory = paragraph.chartHistory;

            // Clear history on query change.
            if (clearChart) {
                chartHistory.length = 0;

                _.forEach(paragraph.charts, (chart) => chart.data.length = 0);
            }

            // Add results to history.
            chartHistory.push({tm: new Date(), rows: paragraph.rows});

            // Keep history size no more than max length.
            while (chartHistory.length > HISTORY_LENGTH)
                chartHistory.shift();

            _showLoading(paragraph, false);

            if (_.isNil(paragraph.result) || paragraph.result === 'none' || paragraph.scanExplain())
                paragraph.result = 'table';
            else if (paragraph.chart()) {
                let resetCharts = clearChart;

                if (!resetCharts) {
                    const curKeyCols = paragraph.chartKeyCols;
                    const curValCols = paragraph.chartValCols;

                    resetCharts = !prevKeyCols || !prevValCols ||
                        prevKeyCols.length !== curKeyCols.length ||
                        prevValCols.length !== curValCols.length;
                }

                _chartApplySettings(paragraph, resetCharts);
            }
        };

        const _closeOldQuery = (paragraph) => {
            const nid = paragraph.resNodeId;

            if (paragraph.queryId && _.find($scope.caches, ({nodes}) => _.find(nodes, {nid: nid.toUpperCase()})))
                return agentMgr.queryClose(nid, paragraph.queryId);

            return $q.when();
        };

        /**
         * @param {String} name Cache name.
         * @return {Array.<String>} Nids
         */
        const cacheNodes = (name) => {
            return _.find($scope.caches, {name}).nodes;
        };

        /**
         * @param {String} name Cache name.
         * @param {Boolean} local Local query.
         * @return {String} Nid
         */
        const _chooseNode = (name, local) => {
            if (_.isEmpty(name))
                return Promise.resolve(null);

            const nodes = _.filter(cacheNodes(name), (node) => !node.client);

            if (local) {
                return Nodes.selectNode(nodes, name)
                    .then((selectedNids) => _.head(selectedNids));
            }

            return Promise.resolve(nodes[_.random(0, nodes.length - 1)].nid);
        };

        const _executeRefresh = (paragraph) => {
            const args = paragraph.queryArgs;

            agentMgr.awaitCluster()
                .then(() => _closeOldQuery(paragraph))
                .then(() => args.localNid || _chooseNode(args.cacheName, false))
                .then((nid) => agentMgr.querySql(nid, args.cacheName, args.query, args.nonCollocatedJoins,
                    args.enforceJoinOrder, false, !!args.localNid, args.pageSize, args.lazy, args.collocated))
                .then((res) => _processQueryResult(paragraph, false, res))
                .catch((err) => paragraph.setError(err));
        };

        const _tryStartRefresh = function(paragraph) {
            _tryStopRefresh(paragraph);

            if (_.get(paragraph, 'rate.installed') && paragraph.queryExecuted()) {
                $scope.chartAcceptKeyColumn(paragraph, TIME_LINE);

                _executeRefresh(paragraph);

                const delay = paragraph.rate.value * paragraph.rate.unit;

                paragraph.rate.stopTime = $interval(_executeRefresh, delay, 0, false, paragraph);
            }
        };

        const addLimit = (query, limitSize) =>
            `SELECT * FROM (
            ${query} 
            ) LIMIT ${limitSize}`;

        $scope.nonCollocatedJoinsAvailable = (paragraph) => {
            const cache = _.find($scope.caches, {name: paragraph.cacheName});

            if (cache)
                return !!_.find(cache.nodes, (node) => Version.since(node.version, NON_COLLOCATED_JOINS_SINCE));

            return false;
        };

        $scope.collocatedJoinsAvailable = (paragraph) => {
            const cache = _.find($scope.caches, {name: paragraph.cacheName});

            if (cache)
                return !!_.find(cache.nodes, (node) => Version.since(node.version, ...COLLOCATED_QUERY_SINCE));

            return false;
        };

        $scope.enforceJoinOrderAvailable = (paragraph) => {
            const cache = _.find($scope.caches, {name: paragraph.cacheName});

            if (cache)
                return !!_.find(cache.nodes, (node) => Version.since(node.version, ...ENFORCE_JOIN_SINCE));

            return false;
        };

        $scope.lazyQueryAvailable = (paragraph) => {
            const cache = _.find($scope.caches, {name: paragraph.cacheName});

            if (cache)
                return !!_.find(cache.nodes, (node) => Version.since(node.version, ...LAZY_QUERY_SINCE));

            return false;
        };

        $scope.ddlAvailable = (paragraph) => {
            const cache = _.find($scope.caches, {name: paragraph.cacheName});

            if (cache)
                return !!_.find(cache.nodes, (node) => Version.since(node.version, ...DDL_SINCE));

            return false;
        };

        $scope.cacheNameForSql = (paragraph) => {
            return $scope.ddlAvailable(paragraph) && !paragraph.useAsDefaultSchema ? null : paragraph.cacheName;
        };

        $scope.execute = (paragraph, local = false) => {
            const nonCollocatedJoins = !!paragraph.nonCollocatedJoins;
            const enforceJoinOrder = !!paragraph.enforceJoinOrder;
            const lazy = !!paragraph.lazy;
            const collocated = !!paragraph.collocated;

            $scope.queryAvailable(paragraph) && _chooseNode(paragraph.cacheName, local)
                .then((nid) => {
                    // If we are executing only selected part of query then Notebook shouldn't be saved.
                    if (!paragraph.partialQuery)
                        Notebook.save($scope.notebook).catch(Messages.showError);

                    paragraph.localQueryMode = local;
                    paragraph.prevQuery = paragraph.queryArgs ? paragraph.queryArgs.query : paragraph.query;

                    _showLoading(paragraph, true);

                    return _closeOldQuery(paragraph)
                        .then(() => {
                            const query = paragraph.partialQuery || paragraph.query;

                            const args = paragraph.queryArgs = {
                                type: 'QUERY',
                                cacheName: $scope.cacheNameForSql(paragraph),
                                query,
                                pageSize: paragraph.pageSize,
                                maxPages: paragraph.maxPages,
                                nonCollocatedJoins,
                                enforceJoinOrder,
                                localNid: local ? nid : null,
                                lazy,
                                collocated
                            };

                            ActivitiesData.post({ action: '/queries/execute' });

                            const qry = args.maxPages ? addLimit(args.query, args.pageSize * args.maxPages) : query;

                            return agentMgr.querySql(nid, args.cacheName, qry, nonCollocatedJoins, enforceJoinOrder, false, local, args.pageSize, lazy, collocated);
                        })
                        .then((res) => {
                            _processQueryResult(paragraph, true, res);

                            _tryStartRefresh(paragraph);
                        })
                        .catch((err) => {
                            paragraph.setError(err);

                            _showLoading(paragraph, false);

                            $scope.stopRefresh(paragraph);
                        })
                        .then(() => paragraph.ace.focus());
                });
        };

        const _cancelRefresh = (paragraph) => {
            if (paragraph.rate && paragraph.rate.stopTime) {
                delete paragraph.queryArgs;

                paragraph.rate.installed = false;

                $interval.cancel(paragraph.rate.stopTime);

                delete paragraph.rate.stopTime;
            }
        };

        $scope.explain = (paragraph) => {
            if (!$scope.queryAvailable(paragraph))
                return;

            Notebook.save($scope.notebook)
                .catch(Messages.showError);

            _cancelRefresh(paragraph);

            _showLoading(paragraph, true);

            _closeOldQuery(paragraph)
                .then(() => _chooseNode(paragraph.cacheName, false))
                .then((nid) => {
                    const args = paragraph.queryArgs = {
                        type: 'EXPLAIN',
                        cacheName: $scope.cacheNameForSql(paragraph),
                        query: 'EXPLAIN ' + paragraph.query,
                        pageSize: paragraph.pageSize
                    };

                    ActivitiesData.post({ action: '/queries/explain' });

                    return agentMgr.querySql(nid, args.cacheName, args.query, args.nonCollocatedJoins, !!paragraph.enforceJoinOrder, false, false, args.pageSize, false, args.collocated);
                })
                .then((res) => _processQueryResult(paragraph, true, res))
                .catch((err) => {
                    paragraph.setError(err);

                    _showLoading(paragraph, false);
                })
                .then(() => paragraph.ace.focus());
        };

        $scope.scan = (paragraph, local = false) => {
            const cacheName = paragraph.cacheName;
            const caseSensitive = !!paragraph.caseSensitive;
            const filter = paragraph.filter;
            const pageSize = paragraph.pageSize;

            $scope.scanAvailable(paragraph) && _chooseNode(cacheName, local)
                .then((nid) => {
                    paragraph.localQueryMode = local;
                    paragraph.scanningInProgress = true;

                    Notebook.save($scope.notebook)
                        .catch(Messages.showError);

                    _cancelRefresh(paragraph);

                    _showLoading(paragraph, true);

                    _closeOldQuery(paragraph)
                        .then(() => {
                            paragraph.queryArgs = {
                                type: 'SCAN',
                                cacheName,
                                filter,
                                regEx: false,
                                caseSensitive,
                                near: false,
                                pageSize,
                                localNid: local ? nid : null
                            };

                            ActivitiesData.post({ action: '/queries/scan' });

                            return agentMgr.queryScan(nid, cacheName, filter, false, caseSensitive, false, local, pageSize);
                        })
                        .then((res) => _processQueryResult(paragraph, true, res))
                        .catch((err) => {
                            paragraph.setError(err);

                            _showLoading(paragraph, false);
                        })
                        .then(() => paragraph.scanningInProgress = false);
                });
        };

        function _updatePieChartsWithData(paragraph, newDatum) {
            $timeout(() => {
                _.forEach(paragraph.charts, function(chart) {
                    const chartDatum = chart.data;

                    chartDatum.length = 0;

                    _.forEach(newDatum, function(series) {
                        if (chart.options.title.text === series.key)
                            _.forEach(series.values, (v) => chartDatum.push(v));
                    });
                });

                _.forEach(paragraph.charts, (chart) => chart.api.update());
            });
        }

        $scope.nextPage = (paragraph) => {
            _showLoading(paragraph, true);

            paragraph.queryArgs.pageSize = paragraph.pageSize;

            agentMgr.queryNextPage(paragraph.resNodeId, paragraph.queryId, paragraph.pageSize)
                .then((res) => {
                    paragraph.page++;

                    paragraph.total += paragraph.rows.length;

                    paragraph.duration = res.duration;

                    paragraph.rows = res.rows;

                    if (paragraph.chart()) {
                        if (paragraph.result === 'pie')
                            _updatePieChartsWithData(paragraph, _pieChartDatum(paragraph));
                        else
                            _updateChartsWithData(paragraph, _chartDatum(paragraph));
                    }

                    paragraph.gridOptions.adjustHeight(paragraph.rows.length);

                    _showLoading(paragraph, false);

                    if (!res.hasMore)
                        delete paragraph.queryId;
                })
                .catch((err) => {
                    paragraph.setError(err);

                    _showLoading(paragraph, false);
                })
                .then(() => paragraph.ace && paragraph.ace.focus());
        };

        const _export = (fileName, columnDefs, meta, rows, toClipBoard = false) => {
            const csvSeparator = this.CSV.getSeparator();
            let csvContent = '';

            const cols = [];
            const excludedCols = [];

            _.forEach(meta, (col, idx) => {
                if (columnDefs[idx].visible)
                    cols.push(_fullColName(col));
                else
                    excludedCols.push(idx);
            });

            csvContent += cols.join(csvSeparator) + '\n';

            _.forEach(rows, (row) => {
                cols.length = 0;

                if (Array.isArray(row)) {
                    _.forEach(row, (elem, idx) => {
                        if (_.includes(excludedCols, idx))
                            return;

                        cols.push(_.isUndefined(elem) ? '' : JSON.stringify(elem));
                    });
                }
                else {
                    _.forEach(columnDefs, (col) => {
                        if (col.visible) {
                            const elem = row[col.fieldName];

                            cols.push(_.isUndefined(elem) ? '' : JSON.stringify(elem));
                        }
                    });
                }

                csvContent += cols.join(csvSeparator) + '\n';
            });

            if (toClipBoard)
                IgniteCopyToClipboard.copy(csvContent);
            else
                LegacyUtils.download('text/csv', fileName, csvContent);
        };

        /**
         * Generate file name with query results.
         *
         * @param paragraph {Object} Query paragraph .
         * @param all {Boolean} All result export flag.
         * @returns {string}
         */
        const exportFileName = (paragraph, all) => {
            const args = paragraph.queryArgs;

            if (args.type === 'SCAN')
                return `export-scan-${args.cacheName}-${paragraph.name}${all ? '-all' : ''}.csv`;

            return `export-query-${paragraph.name}${all ? '-all' : ''}.csv`;
        };

        $scope.exportCsvToClipBoard = (paragraph) => {
            _export(exportFileName(paragraph, false), paragraph.gridOptions.columnDefs, paragraph.meta, paragraph.rows, true);
        };

        $scope.exportCsv = function(paragraph) {
            _export(exportFileName(paragraph, false), paragraph.gridOptions.columnDefs, paragraph.meta, paragraph.rows);

            // paragraph.gridOptions.api.exporter.csvExport(uiGridExporterConstants.ALL, uiGridExporterConstants.VISIBLE);
        };

        $scope.exportPdf = function(paragraph) {
            paragraph.gridOptions.api.exporter.pdfExport(uiGridExporterConstants.ALL, uiGridExporterConstants.VISIBLE);
        };

        $scope.exportCsvAll = (paragraph) => {
            paragraph.csvIsPreparing = true;

            const args = paragraph.queryArgs;

            return Promise.resolve(args.localNid || _chooseNode(args.cacheName, false))
                .then((nid) => args.type === 'SCAN'
                    ? agentMgr.queryScanGetAll(nid, args.cacheName, args.query, !!args.regEx, !!args.caseSensitive, !!args.near, !!args.localNid)
                    : agentMgr.querySqlGetAll(nid, args.cacheName, args.query, !!args.nonCollocatedJoins, !!args.enforceJoinOrder, false, !!args.localNid, !!args.lazy, !!args.collocated))
                .then((res) => _export(exportFileName(paragraph, true), paragraph.gridOptions.columnDefs, res.columns, res.rows))
                .catch(Messages.showError)
                .then(() => {
                    paragraph.csvIsPreparing = false;

                    return paragraph.ace && paragraph.ace.focus();
                });
        };

        // $scope.exportPdfAll = function(paragraph) {
        //    $http.post('/api/v1/agent/query/getAll', {query: paragraph.query, cacheName: paragraph.cacheName})
        //    .then(({data}) {
        //        _export(paragraph.name + '-all.csv', data.meta, data.rows);
        //    })
        //    .catch(Messages.showError);
        // };

        $scope.rateAsString = function(paragraph) {
            if (paragraph.rate && paragraph.rate.installed) {
                const idx = _.findIndex($scope.timeUnit, function(unit) {
                    return unit.value === paragraph.rate.unit;
                });

                if (idx >= 0)
                    return ' ' + paragraph.rate.value + $scope.timeUnit[idx].short;

                paragraph.rate.installed = false;
            }

            return '';
        };

        $scope.startRefresh = function(paragraph, value, unit) {
            paragraph.rate.value = value;
            paragraph.rate.unit = unit;
            paragraph.rate.installed = true;

            if (paragraph.queryExecuted() && !paragraph.scanExplain())
                _tryStartRefresh(paragraph);
        };

        $scope.stopRefresh = function(paragraph) {
            paragraph.rate.installed = false;

            _tryStopRefresh(paragraph);
        };

        $scope.paragraphTimeSpanVisible = function(paragraph) {
            return paragraph.timeLineSupported() && paragraph.chartTimeLineEnabled();
        };

        $scope.paragraphTimeLineSpan = function(paragraph) {
            if (paragraph && paragraph.timeLineSpan)
                return paragraph.timeLineSpan.toString();

            return '1';
        };

        $scope.applyChartSettings = function(paragraph) {
            _chartApplySettings(paragraph, true);
        };

        $scope.queryAvailable = function(paragraph) {
            return paragraph.query && !paragraph.loading;
        };

        $scope.queryTooltip = function(paragraph, action) {
            if ($scope.queryAvailable(paragraph))
                return;

            if (paragraph.loading)
                return 'Waiting for server response';

            return 'Input text to ' + action;
        };

        $scope.scanAvailable = function(paragraph) {
            return $scope.caches.length && !(paragraph.loading || paragraph.csvIsPreparing);
        };

        $scope.scanTooltip = function(paragraph) {
            if ($scope.scanAvailable(paragraph))
                return;

            if (paragraph.loading)
                return 'Waiting for server response';

            return 'Select cache to export scan results';
        };

        $scope.clickableMetadata = function(node) {
            return node.type.slice(0, 5) !== 'index';
        };

        $scope.dblclickMetadata = function(paragraph, node) {
            paragraph.ace.insert(node.name);

            setTimeout(() => paragraph.ace.focus(), 1);
        };

        $scope.importMetadata = function() {
            Loading.start('loadingCacheMetadata');

            $scope.metadata = [];

            agentMgr.metadata()
                .then((metadata) => {
                    $scope.metadata = _.sortBy(_.filter(metadata, (meta) => {
                        const cache = _.find($scope.caches, { name: meta.cacheName });

                        if (cache) {
                            meta.name = (cache.sqlSchema || '"' + meta.cacheName + '"') + '.' + meta.typeName;
                            meta.displayName = (cache.sqlSchema || meta.maskedName) + '.' + meta.typeName;

                            if (cache.sqlSchema)
                                meta.children.unshift({type: 'plain', name: 'cacheName: ' + meta.maskedName, maskedName: meta.maskedName});

                            meta.children.unshift({type: 'plain', name: 'mode: ' + cache.mode, maskedName: meta.maskedName});
                        }

                        return cache;
                    }), 'name');
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('loadingCacheMetadata'));
        };

        $scope.showResultQuery = function(paragraph) {
            if (!_.isNil(paragraph)) {
                const scope = $scope.$new();

                if (paragraph.queryArgs.type === 'SCAN') {
                    scope.title = 'SCAN query';

                    const filter = paragraph.queryArgs.filter;

                    if (_.isEmpty(filter))
                        scope.content = [`SCAN query for cache: <b>${maskCacheName(paragraph.queryArgs.cacheName, true)}</b>`];
                    else
                        scope.content = [`SCAN query for cache: <b>${maskCacheName(paragraph.queryArgs.cacheName, true)}</b> with filter: <b>${filter}</b>`];
                }
                else if (paragraph.queryArgs.query .startsWith('EXPLAIN ')) {
                    scope.title = 'Explain query';
                    scope.content = paragraph.queryArgs.query.split(/\r?\n/);
                }
                else {
                    scope.title = 'SQL query';
                    scope.content = paragraph.queryArgs.query.split(/\r?\n/);
                }

                // Attach duration and selected node info
                scope.meta = `Duration: ${$filter('duration')(paragraph.duration)}.`;
                scope.meta += paragraph.localQueryMode ? ` Node ID8: ${id8(paragraph.resNodeId)}` : '';

                // Show a basic modal from a controller
                $modal({scope, templateUrl: messageTemplateUrl, show: true});
            }
        };

        $scope.showStackTrace = function(paragraph) {
            if (!_.isNil(paragraph)) {
                const scope = $scope.$new();

                scope.title = 'Error details';
                scope.content = [];

                const tab = '&nbsp;&nbsp;&nbsp;&nbsp;';

                const addToTrace = (item) => {
                    if (nonNil(item)) {
                        scope.content.push((scope.content.length > 0 ? tab : '') + errorParser.extractFullMessage(item));

                        addToTrace(item.cause);

                        _.forEach(item.suppressed, (sup) => addToTrace(sup));
                    }
                };

                addToTrace(paragraph.error.root);

                // Show a basic modal from a controller
                $modal({scope, templateUrl: messageTemplateUrl, show: true});
            }
        };
    }

    $onInit() {

    }

    $onDestroy() {
        this.refresh$.unsubscribe();
    }
}
