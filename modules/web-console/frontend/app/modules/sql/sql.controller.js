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

// Time line X axis descriptor.
const TIME_LINE = {value: -1, type: 'java.sql.Date', label: 'TIME_LINE'};

// Row index X axis descriptor.
const ROW_IDX = {value: -2, type: 'java.lang.Integer', label: 'ROW_IDX'};

/** Prefix for node local key for SCAN near queries. */
const SCAN_CACHE_WITH_FILTER = 'VISOR_SCAN_CACHE_WITH_FILTER';

/** Prefix for node local key for SCAN near queries. */
const SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE = 'VISOR_SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE';

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
    constructor($animate, $timeout, paragraph) {
        const self = this;

        self.id = 'paragraph-' + paragraphId++;

        _.assign(this, paragraph);

        Object.defineProperty(this, 'gridOptions', {value: {
            enableGridMenu: false,
            enableColumnMenus: false,
            flatEntityAccess: true,
            fastWatch: true,
            rebuildColumns() {
                if (_.isNil(this.api))
                    return;

                this.columnDefs = _.reduce(self.meta, (cols, col, idx) => {
                    if (self.columnFilter(col)) {
                        cols.push({
                            displayName: col.fieldName,
                            headerTooltip: _fullColName(col),
                            field: idx.toString(),
                            minWidth: 50,
                            cellClass: 'cell-left'
                        });
                    }

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
    }

    resultType() {
        if (_.isNil(this.queryArgs))
            return null;

        if (!_.isEmpty(this.errMsg))
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
        return !_.isEmpty(this.meta);
    }

    scanExplain() {
        return this.queryExecuted() && this.queryArgs.type !== 'QUERY';
    }

    timeLineSupported() {
        return this.result !== 'pie';
    }

    chartColumnsConfigured() {
        return !_.isEmpty(this.chartKeyCols) && !_.isEmpty(this.chartValCols);
    }

    chartTimeLineEnabled() {
        return !_.isEmpty(this.chartKeyCols) && _.eq(this.chartKeyCols[0], TIME_LINE);
    }
}

// Controller for SQL notebook screen.
export default ['$rootScope', '$scope', '$http', '$q', '$timeout', '$interval', '$animate', '$location', '$anchorScroll', '$state', '$modal', '$popover', 'IgniteLoading', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteConfirm', 'IgniteAgentMonitor', 'IgniteChartColors', 'IgniteNotebook', 'IgniteScanFilterInput', 'uiGridExporterConstants',
    function($root, $scope, $http, $q, $timeout, $interval, $animate, $location, $anchorScroll, $state, $modal, $popover, Loading, LegacyUtils, Messages, Confirm, agentMonitor, IgniteChartColors, Notebook, ScanFilterInput, uiGridExporterConstants) {
        let stopTopology = null;

        const _tryStopRefresh = function(paragraph) {
            if (paragraph.rate && paragraph.rate.stopTime) {
                $interval.cancel(paragraph.rate.stopTime);

                delete paragraph.rate.stopTime;
            }
        };

        const _stopTopologyRefresh = () => {
            $interval.cancel(stopTopology);

            if ($scope.notebook && $scope.notebook.paragraphs)
                $scope.notebook.paragraphs.forEach((paragraph) => _tryStopRefresh(paragraph));
        };

        $scope.$on('$stateChangeStart', _stopTopologyRefresh);

        $scope.caches = [];

        $scope.pageSizes = [50, 100, 200, 400, 800, 1000];

        $scope.timeLineSpans = ['1', '5', '10', '15', '30'];

        $scope.aggregateFxs = ['FIRST', 'LAST', 'MIN', 'MAX', 'SUM', 'AVG', 'COUNT'];

        $scope.modes = LegacyUtils.mkOptions(['PARTITIONED', 'REPLICATED', 'LOCAL']);

        $scope.loadingText = $root.IgniteDemoMode ? 'Demo grid is starting. Please wait...' : 'Loading notebook screen...';

        $scope.timeUnit = [
            {value: 1000, label: 'seconds', short: 's'},
            {value: 60000, label: 'minutes', short: 'm'},
            {value: 3600000, label: 'hours', short: 'h'}
        ];

        $scope.exportDropdown = [
            { text: 'Export all', click: 'exportCsvAll(paragraph)' }
            // { 'text': 'Export all to CSV', 'click': 'exportCsvAll(paragraph)' },
            // { 'text': 'Export all to PDF', 'click': 'exportPdfAll(paragraph)' }
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

        $scope.maskCacheName = (cacheName) => _.isEmpty(cacheName) ? '<default>' : cacheName;

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
                            margin: {right: -25}
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
                                margin: {
                                    right: -25
                                }
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
                            margin: {
                                right: -25
                            }
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
                            margin: {right: -25}
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
                    $scope.notebook.expandedParagraphs.push(idx);

                setTimeout(function() {
                    $scope.notebook.paragraphs[idx].ace.focus();
                });
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
         * @private
         */
        const _refreshFn = () =>
            agentMonitor.topology()
                .then((clusters) => {
                    $scope.caches = _.sortBy(_.reduce(clusters, (items, cluster) => {
                        _.forEach(cluster.caches, (cache) => {
                            let item = _.find(items, {name: cache.name});

                            if (_.isNil(item)) {
                                cache.label = $scope.maskCacheName(cache.name);

                                cache.nodeIds = [];

                                items.push(item = cache);
                            }

                            item.nodeIds.push(cluster.nodeId);
                        });

                        return items;
                    }, []), 'label');

                    if (_.isEmpty($scope.caches))
                        return;

                    // Reset to first cache in case of stopped selected.
                    const cacheNames = _.map($scope.caches, (cache) => cache.name);

                    _.forEach($scope.notebook.paragraphs, (paragraph) => {
                        if (!_.includes(cacheNames, paragraph.cacheName))
                            paragraph.cacheName = _.head(cacheNames);
                    });
                })
                .then(() => agentMonitor.checkModal())
                .catch((err) => agentMonitor.showNodeError(err));

        const _startWatch = () =>
            agentMonitor.startWatch({
                state: 'base.configuration.clusters',
                text: 'Back to Configuration',
                goal: 'execute sql statements',
                onDisconnect: () => {
                    _stopTopologyRefresh();

                    _startWatch();
                }
            })
                .then(() => Loading.start('sqlLoading'))
                .then(_refreshFn)
                .then(() => Loading.finish('sqlLoading'))
                .then(() => {
                    $root.IgniteDemoMode && _.forEach($scope.notebook.paragraphs, $scope.execute);

                    stopTopology = $interval(_refreshFn, 5000, 0, false);
                });

        Notebook.find($state.params.noteId)
            .then((notebook) => {
                $scope.notebook = _.cloneDeep(notebook);

                $scope.notebook_name = $scope.notebook.name;

                if (!$scope.notebook.expandedParagraphs)
                    $scope.notebook.expandedParagraphs = [];

                if (!$scope.notebook.paragraphs)
                    $scope.notebook.paragraphs = [];

                $scope.notebook.paragraphs = _.map($scope.notebook.paragraphs,
                    (paragraph) => new Paragraph($animate, $timeout, paragraph));

                if (_.isEmpty($scope.notebook.paragraphs))
                    $scope.addParagraph();
                else
                    $scope.rebuildScrollParagraphs();
            })
            .then(_startWatch)
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

        $scope.addParagraph = function() {
            const sz = $scope.notebook.paragraphs.length;

            const paragraph = new Paragraph($animate, $timeout, {
                name: 'Query' + (sz === 0 ? '' : sz),
                query: '',
                pageSize: $scope.pageSizes[0],
                timeLineSpan: $scope.timeLineSpans[0],
                result: 'none',
                rate: {
                    value: 1,
                    unit: 60000,
                    installed: false
                }
            });

            if ($scope.caches && $scope.caches.length > 0)
                paragraph.cacheName = $scope.caches[0].name;

            $scope.notebook.paragraphs.push(paragraph);

            $scope.notebook.expandedParagraphs.push(sz);

            $scope.rebuildScrollParagraphs();

            $location.hash(paragraph.id);

            $timeout(() => {
                $anchorScroll();

                paragraph.ace.focus();
            });
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
            Confirm.confirm('Are you sure you want to remove: "' + paragraph.name + '"?')
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
                if (paragraph.columnFilter(col) && _notObjectType(col.fieldTypeName)) {
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

            paragraph.systemColumns = !paragraph.systemColumns;

            paragraph.columnFilter = _columnFilter(paragraph);

            paragraph.chartColumns = [];

            _rebuildColumns(paragraph);
        };

        const _showLoading = (paragraph, enable) => paragraph.loading = enable;

        /**
         * @param {Object} paragraph Query
         * @param {{columns: Array, rows: Array, responseNodeId: String, queryId: int, hasMore: Boolean}} res Query results.
         * @private
         */
        const _processQueryResult = (paragraph, res) => {
            const prevKeyCols = paragraph.chartKeyCols;
            const prevValCols = paragraph.chartValCols;

            if (!_.eq(paragraph.meta, res.columns)) {
                paragraph.meta = [];

                paragraph.chartColumns = [];

                if (!LegacyUtils.isDefined(paragraph.chartKeyCols))
                    paragraph.chartKeyCols = [];

                if (!LegacyUtils.isDefined(paragraph.chartValCols))
                    paragraph.chartValCols = [];

                if (res.columns.length <= 2) {
                    const _key = _.find(res.columns, {fieldName: '_KEY'});
                    const _val = _.find(res.columns, {fieldName: '_VAL'});

                    paragraph.disabledSystemColumns = (res.columns.length === 2 && _key && _val) ||
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

            delete paragraph.errMsg;

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
            const queryChanged = paragraph.prevQuery !== paragraph.query;

            if (queryChanged) {
                paragraph.prevQuery = paragraph.query;

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
                let resetCharts = queryChanged;

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
            const queryId = paragraph.queryArgs && paragraph.queryArgs.queryId;

            return queryId ? agentMonitor.queryClose(queryId) : $q.when();
        };

        const cacheNode = (name) => {
            const cache = _.find($scope.caches, {name});

            return cache.nodeIds[_.random(0, cache.nodeIds.length - 1)];
        };

        const _executeRefresh = (paragraph) => {
            const args = paragraph.queryArgs;

            agentMonitor.awaitAgent()
                .then(() => _closeOldQuery(paragraph))
                .then(() => agentMonitor.query(cacheNode(args.cacheName), args.cacheName, args.query, false, args.pageSize))
                .then(_processQueryResult.bind(this, paragraph))
                .catch((err) => paragraph.errMsg = err.message);
        };

        const _tryStartRefresh = function(paragraph) {
            _tryStopRefresh(paragraph);

            if (paragraph.rate && paragraph.rate.installed && paragraph.queryArgs) {
                $scope.chartAcceptKeyColumn(paragraph, TIME_LINE);

                _executeRefresh(paragraph);

                const delay = paragraph.rate.value * paragraph.rate.unit;

                paragraph.rate.stopTime = $interval(_executeRefresh, delay, 0, false, paragraph);
            }
        };

        $scope.execute = (paragraph) => {
            if (!$scope.actionAvailable(paragraph, true))
                return;

            Notebook.save($scope.notebook)
                .catch(Messages.showError);

            paragraph.prevQuery = paragraph.queryArgs ? paragraph.queryArgs.query : paragraph.query;

            _showLoading(paragraph, true);

            _closeOldQuery(paragraph)
                .then(() => {
                    const args = paragraph.queryArgs = {
                        cacheName: paragraph.cacheName,
                        pageSize: paragraph.pageSize,
                        query: paragraph.query,
                        type: 'QUERY'
                    };

                    return agentMonitor.query(cacheNode(paragraph.cacheName), args.cacheName, args.query, false, args.pageSize);
                })
                .then((res) => {
                    _processQueryResult(paragraph, res);

                    _tryStartRefresh(paragraph);
                })
                .catch((err) => {
                    paragraph.errMsg = err.message;

                    _showLoading(paragraph, false);

                    $scope.stopRefresh(paragraph);
                })
                .then(() => paragraph.ace.focus());
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
            if (!$scope.actionAvailable(paragraph, true))
                return;

            Notebook.save($scope.notebook)
                .catch(Messages.showError);

            _cancelRefresh(paragraph);

            _showLoading(paragraph, true);

            _closeOldQuery(paragraph)
                .then(() => {
                    const args = paragraph.queryArgs = {
                        cacheName: paragraph.cacheName,
                        pageSize: paragraph.pageSize,
                        query: 'EXPLAIN ' + paragraph.query,
                        type: 'EXPLAIN'
                    };

                    return agentMonitor.query(cacheNode(paragraph.cacheName), args.cacheName, args.query, false, args.pageSize);
                })
                .then(_processQueryResult.bind(this, paragraph))
                .catch((err) => {
                    paragraph.errMsg = err.message;

                    _showLoading(paragraph, false);
                })
                .then(() => paragraph.ace.focus());
        };

        $scope.scan = (paragraph, query = null) => {
            if (!$scope.actionAvailable(paragraph, false))
                return;

            Notebook.save($scope.notebook)
                .catch(Messages.showError);

            _cancelRefresh(paragraph);

            _showLoading(paragraph, true);

            _closeOldQuery(paragraph)
                .then(() => {
                    const args = paragraph.queryArgs = {
                        cacheName: paragraph.cacheName,
                        pageSize: paragraph.pageSize,
                        query,
                        type: 'SCAN'
                    };

                    return agentMonitor.query(cacheNode(paragraph.cacheName), args.cacheName, query, false, args.pageSize);
                })
                .then(_processQueryResult.bind(this, paragraph))
                .catch((err) => {
                    paragraph.errMsg = err.message;

                    _showLoading(paragraph, false);
                })
                .then(() => paragraph.ace.focus());
        };

        $scope.scanWithFilter = (paragraph) => {
            if (!$scope.actionAvailable(paragraph, false))
                return;

            ScanFilterInput.open()
                .then(({filter, caseSensitive}) => {
                    const prefix = caseSensitive ? SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE : SCAN_CACHE_WITH_FILTER;

                    $scope.scan(paragraph, `${prefix}${filter}`);
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

            agentMonitor.next(paragraph.resNodeId, paragraph.queryId, paragraph.pageSize)
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
                    paragraph.errMsg = err.message;

                    _showLoading(paragraph, false);
                })
                .then(() => paragraph.ace.focus());
        };

        const _export = (fileName, columnFilter, meta, rows) => {
            let csvContent = '';

            const cols = [];
            const excludedCols = [];

            if (meta) {
                _.forEach(meta, (col, idx) => {
                    if (columnFilter(col))
                        cols.push(_fullColName(col));
                    else
                        excludedCols.push(idx);
                });

                csvContent += cols.join(';') + '\n';
            }

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
                    _.forEach(meta, (col) => {
                        if (columnFilter(col)) {
                            const elem = row[col.fieldName];

                            cols.push(_.isUndefined(elem) ? '' : JSON.stringify(elem));
                        }
                    });
                }

                csvContent += cols.join(';') + '\n';
            });

            LegacyUtils.download('application/octet-stream;charset=utf-8', fileName, escape(csvContent));
        };

        $scope.exportCsv = function(paragraph) {
            _export(paragraph.name + '.csv', paragraph.columnFilter, paragraph.meta, paragraph.rows);

            // paragraph.gridOptions.api.exporter.csvExport(uiGridExporterConstants.ALL, uiGridExporterConstants.VISIBLE);
        };

        $scope.exportPdf = function(paragraph) {
            paragraph.gridOptions.api.exporter.pdfExport(uiGridExporterConstants.ALL, uiGridExporterConstants.VISIBLE);
        };

        $scope.exportCsvAll = function(paragraph) {
            const args = paragraph.queryArgs;

            agentMonitor.queryGetAll(cacheNode(args.cacheName), args.cacheName, args.query, false)
                .then((res) => _export(paragraph.name + '-all.csv', paragraph.columnFilter, res.columns, res.rows))
                .catch(Messages.showError)
                .then(() => paragraph.ace.focus());
        };

        // $scope.exportPdfAll = function(paragraph) {
        //    $http.post('/api/v1/agent/query/getAll', {query: paragraph.query, cacheName: paragraph.cacheName})
        //        .success(function(item) {
        //            _export(paragraph.name + '-all.csv', item.meta, item.rows);
        //    })
        //    .error(Messages.showError);
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

            if (paragraph.queryExecuted())
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

        $scope.actionAvailable = function(paragraph, needQuery) {
            return $scope.caches.length > 0 && (!needQuery || paragraph.query) && !paragraph.loading;
        };

        $scope.actionTooltip = function(paragraph, action, needQuery) {
            if ($scope.actionAvailable(paragraph, needQuery))
                return;

            if (paragraph.loading)
                return 'Waiting for server response';

            return 'To ' + action + ' query select cache' + (needQuery ? ' and input query' : '');
        };

        $scope.clickableMetadata = function(node) {
            return node.type.slice(0, 5) !== 'index';
        };

        $scope.dblclickMetadata = function(paragraph, node) {
            paragraph.ace.insert(node.name);

            setTimeout(function() {
                paragraph.ace.focus();
            }, 1);
        };

        $scope.importMetadata = function() {
            Loading.start('loadingCacheMetadata');

            $scope.metadata = [];

            agentMonitor.metadata()
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

                if (_.isNil(paragraph.queryArgs.query)) {
                    scope.title = 'SCAN query';
                    scope.content = [`SCAN query for cache: <b>${$scope.maskCacheName(paragraph.queryArgs.cacheName)}</b>`];
                }
                else if (paragraph.queryArgs.query.startsWith(SCAN_CACHE_WITH_FILTER)) {
                    scope.title = 'SCAN query';

                    let filter = '';

                    if (paragraph.queryArgs.query.startsWith(SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE))
                        filter = paragraph.queryArgs.query.substr(SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE.length);
                    else
                        filter = paragraph.queryArgs.query.substr(SCAN_CACHE_WITH_FILTER.length);

                    scope.content = [`SCAN query for cache: <b>${$scope.maskCacheName(paragraph.queryArgs.cacheName)}</b> with filter: <b>${filter}</b>`];
                }
                else if (paragraph.queryArgs.query .startsWith('EXPLAIN ')) {
                    scope.title = 'Explain query';
                    scope.content = [paragraph.queryArgs.query];
                }
                else {
                    scope.title = 'SQL query';
                    scope.content = [paragraph.queryArgs.query];
                }

                // Show a basic modal from a controller
                $modal({scope, template: '/templates/message.html', placement: 'center', show: true});
            }
        };
    }
];
