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

// Controller for SQL notebook screen.
consoleModule.controller('sqlController',
    ['$scope', '$window','$controller', '$http', '$timeout', '$common', '$confirm', '$interval', '$popover', '$loading',
    function ($scope, $window, $controller, $http, $timeout, $common, $confirm, $interval, $popover, $loading) {
    // Initialize the super class and extend it.
    angular.extend(this, $controller('agent-download', {$scope: $scope}));

    $scope.agentGoal = 'execute sql statements';
    $scope.agentTestDriveOption = '--test-drive-sql';

    $scope.joinTip = $common.joinTip;

    $scope.caches = [];

    $scope.pageSizes = [50, 100, 200, 400, 800, 1000];

    $scope.timeLineSpans = ['1', '5', '10', '15', '30'];

    $scope.modes = $common.mkOptions(['PARTITIONED', 'REPLICATED', 'LOCAL']);

    $scope.timeUnit = [
        {value: 1000, label: 'seconds', short: 's'},
        {value: 60000, label: 'minutes', short: 'm'},
        {value: 3600000, label: 'hours', short: 'h'}
    ];

    $scope.exportDropdown = [{ 'text': 'Export all', 'click': 'exportAll(paragraph)'}];

    $scope.treeOptions = {
        nodeChildren: 'children',
        dirSelectable: true,
        injectClasses: {
            iExpanded: 'fa fa-minus-square-o',
            iCollapsed: 'fa fa-plus-square-o'
        }
    };

    // Time line X axis descriptor.
    var TIME_LINE = {value: -1, type: 'java.sql.Date', label: 'TIME_LINE'};

    var chartHistory = [];

    // We need max 1800 items to hold history for 30 mins in case of refresh every second.
    var HISTORY_LENGTH = 1800;

    $scope.chartRemoveKeyColumn = function (paragraph, index) {
        paragraph.chartKeyCols.splice(index, 1);

        _chartApplySettings(paragraph, true);
    };

    $scope.chartRemoveValColumn = function (paragraph, index) {
        paragraph.chartValCols.splice(index, 1);

        _chartApplySettings(paragraph, true);
    };

    $scope.chartAcceptKeyColumn = function(paragraph, item) {
        var accepted = _.findIndex(paragraph.chartKeyCols, item) < 0;

        if (accepted) {
            paragraph.chartKeyCols = [item];

            _chartApplySettings(paragraph, true);
        }

        return false;
    };

    $scope.chartAcceptValColumn = function(paragraph, item) {
        var accepted = _.findIndex(paragraph.chartValCols, item) < 0 && item != TIME_LINE && _numberType(item.type);

        if (accepted) {
            paragraph.chartValCols.push(item);

            _chartApplySettings(paragraph, true);
        }

        return false;
    };

    var _hideColumn = function (col) {
        return !(col.fieldName === '_KEY') && !(col.fieldName == '_VAL');
    };

    var _allColumn = function () {
        return true;
    };

    var paragraphId = 0;

    function enhanceParagraph(paragraph) {
        paragraph.nonEmpty = function () {
            return this.rows && this.rows.length > 0;
        };

        paragraph.chart = function () {
            return this.result != 'table' && this.result != 'none';
        };

        paragraph.queryExecute = function () {
            return this.queryArgs && this.queryArgs.type == 'QUERY';
        };

        paragraph.table = function () {
            return this.result == 'table';
        };

        paragraph.chartColumnsConfigured = function () {
            return !$common.isEmptyArray(this.chartKeyCols) && !$common.isEmptyArray(this.chartValCols);
        };

        paragraph.chartTimeLineEnabled = function () {
            return !$common.isEmptyArray(this.chartKeyCols) && angular.equals(this.chartKeyCols[0], TIME_LINE);
        };

        paragraph.timeLineSupported = function () {
            return this.result != 'pie';
        };

        Object.defineProperty(paragraph, 'gridOptions', { value: {
            enableColResize: true,
            columnDefs: [],
            rowData: null
        }});
    }

    $scope.aceInit = function (paragraph) {
        return function (editor) {
            editor.setAutoScrollEditorIntoView(true);
            editor.$blockScrolling = Infinity;

            var renderer = editor.renderer;

            renderer.setHighlightGutterLine(false);
            renderer.setShowPrintMargin(false);
            renderer.setOption('fontFamily', 'monospace');
            renderer.setOption('fontSize', '14px');
            renderer.setOption('minLines', '5');
            renderer.setOption('maxLines', '15');

            editor.setTheme('ace/theme/chrome');

            Object.defineProperty(paragraph, 'ace', { value: editor });
        }
    };

    var _setActiveCache = function () {
        if ($scope.caches.length > 0)
            _.forEach($scope.notebook.paragraphs, function (paragraph) {
                if (!paragraph.cacheName || !_.find($scope.caches, {name: paragraph.cacheName}))
                    paragraph.cacheName = $scope.caches[0].name;
            });
    };

    var loadNotebook = function () {
        $http.post('/notebooks/get', {noteId: $scope.noteId})
            .success(function (notebook) {
                $scope.notebook = notebook;

                $scope.notebook_name = notebook.name;

                _.forEach(notebook.paragraphs, function (paragraph) {
                    paragraph.id = paragraphId++;

                    enhanceParagraph(paragraph);
                });

                if (!notebook.paragraphs || notebook.paragraphs.length == 0)
                    $scope.addParagraph();

                _setActiveCache();
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    loadNotebook();

    var _saveNotebook = function (f) {
        $http.post('/notebooks/save', $scope.notebook)
            .success(f || function() {})
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.renameNotebook = function (name) {
        if (!name)
            return;

        if ($scope.notebook.name != name) {
            $scope.notebook.name = name;

            _saveNotebook(function () {
                var idx = _.findIndex($scope.$root.notebooks, function (item) {
                    return item._id == $scope.notebook._id;
                });

                if (idx >= 0) {
                    $scope.$root.notebooks[idx].name = name;

                    $scope.$root.rebuildDropdown();
                }

                $scope.notebook.edit = false;
            });
        }
        else
            $scope.notebook.edit = false
    };

    $scope.removeNotebook = function () {
        $confirm.confirm('Are you sure you want to remove: "' + $scope.notebook.name + '"?')
            .then(function () {
                $http.post('/notebooks/remove', {_id: $scope.notebook._id})
                    .success(function () {
                        var idx = _.findIndex($scope.$root.notebooks, function (item) {
                            return item._id == $scope.notebook._id;
                        });

                        if (idx >= 0) {
                            $scope.$root.notebooks.splice(idx, 1);

                            if ($scope.$root.notebooks.length > 0)
                                $window.location = '/sql/' +
                                    $scope.$root.notebooks[Math.min(idx,  $scope.$root.notebooks.length - 1)]._id;
                            else
                                $window.location = '/configuration/clusters';
                        }
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            });
    };

    $scope.renameParagraph = function (paragraph, newName) {
        if (!newName)
            return;

        if (paragraph.name != newName) {
            paragraph.name = newName;

            _saveNotebook(function () { paragraph.edit = false; });
        }
        else
            paragraph.edit = false
    };

    $scope.addParagraph = function () {
        if (!$scope.notebook.paragraphs)
            $scope.notebook.paragraphs = [];

        var sz = $scope.notebook.paragraphs.length;

        var paragraph = {
            id: paragraphId++,
            name: 'Query' + (sz ==0 ? '' : sz),
            editor: true,
            query: '',
            pageSize: $scope.pageSizes[0],
            timeLineSpan: $scope.timeLineSpans[0],
            result: 'none',
            rate: {
                value: 1,
                unit: 60000,
                installed: false
            }
        };

        enhanceParagraph(paragraph);

        if ($scope.caches && $scope.caches.length > 0)
            paragraph.cacheName = $scope.caches[0].name;

        $scope.notebook.expandedParagraphs.push($scope.notebook.paragraphs.length);

        $scope.notebook.paragraphs.push(paragraph);
    };

    $scope.setResult = function (paragraph, new_result) {
        var changed = paragraph.result != new_result;

        paragraph.result = paragraph.result === new_result ? 'none' : new_result;

        if (changed) {
            if (paragraph.chart())
                _chartApplySettings(paragraph, changed);
            else
                setTimeout(function () {
                    paragraph.gridOptions.api.sizeColumnsToFit();
                });
        }
    };

    $scope.resultEq = function(paragraph, result) {
        return (paragraph.result === result);
    };

    $scope.removeParagraph = function(paragraph) {
        $confirm.confirm('Are you sure you want to remove: "' + paragraph.name + '"?')
            .then(function () {
                    var paragraph_idx = _.findIndex($scope.notebook.paragraphs, function (item) {
                        return paragraph == item;
                    });

                    var panel_idx = _.findIndex($scope.notebook.expandedParagraphs, function (item) {
                        return paragraph_idx == item;
                    });

                    if (panel_idx >= 0)
                        $scope.notebook.expandedParagraphs.splice(panel_idx, 1);

                    $scope.notebook.paragraphs.splice(paragraph_idx, 1);
            });
    };

    function getTopology(onSuccess, onException) {
        $http.post('/agent/topology')
            .success(function (caches) {
                onSuccess();

                var oldCaches = $scope.caches;

                $scope.caches = _.sortBy(caches, 'name');

                _.forEach(caches, function (cache) {
                    var old = _.find(oldCaches, { name: cache.name });

                    if (old && old.metadata)
                        cache.metadata = old.metadata;
                });

                _setActiveCache();
            })
            .error(function (err, status) {
                onException(err, status);
            });
    }

    $scope.checkNodeConnection(getTopology);

    var _columnFilter = function(paragraph) {
        return paragraph.disabledSystemColumns || paragraph.systemColumns ? _allColumn : _hideColumn;
    };

    var _notObjectType = function(cls) {
        return $common.isJavaBuildInClass(cls);
    };

    var _numberClasses = ['java.math.BigDecimal', 'java.lang.Byte', 'java.lang.Double',
        'java.lang.Float', 'java.lang.Integer', 'java.lang.Long', 'java.lang.Short'];

    var _numberType = function(cls) {
        return _.contains(_numberClasses, cls);
    };

    var _rebuildColumns = function (paragraph) {
        var columnDefs = [];

        _.forEach(paragraph.meta, function (meta, idx) {
            if (paragraph.columnFilter(meta)) {
                if (_notObjectType(meta.fieldTypeName))
                    paragraph.chartColumns.push({value: idx, type: meta.fieldTypeName, label: meta.fieldName});

                // Index for explain, execute and fieldName for scan.
                var colValue = 'data[' +  (paragraph.queryArgs.query ? idx : '"' + meta.fieldName + '"') + ']';

                columnDefs.push({
                    headerName: meta.fieldName,
                    valueGetter: $common.isJavaBuildInClass(meta.fieldTypeName) ? colValue : 'JSON.stringify(' + colValue + ')',
                    minWidth: 50
                });
            }
        });

        paragraph.gridOptions.api.setColumnDefs(columnDefs);

        if (paragraph.chartColumns.length > 0)
            paragraph.chartColumns.push(TIME_LINE);

        // We could accept onl not object columns for X axis.
        paragraph.chartKeyCols = _retainColumns(paragraph.chartColumns, paragraph.chartKeyCols, _notObjectType);

        // We could accept only numeric columns for Y axis.
        paragraph.chartValCols = _retainColumns(paragraph.chartColumns, paragraph.chartValCols, _numberType, paragraph.chartKeyCols[0]);
    };

    $scope.toggleSystemColumns = function (paragraph) {
        if (paragraph.disabledSystemColumns)
            return;

        paragraph.systemColumns = !paragraph.systemColumns;

        paragraph.columnFilter = _columnFilter(paragraph);

        paragraph.chartColumns = [];

        _rebuildColumns(paragraph);

        setTimeout(function () {
            paragraph.gridOptions.api.sizeColumnsToFit();
        });
    };

    function _retainColumns(allCols, curCols, acceptableType, dfltCol) {
        var retainedCols = [];

        var allColsLen = allCols.length;

        if (allColsLen > 0) {
            curCols.forEach(function (curCol) {
                var col = _.find(allCols, {label: curCol.label});

                if (col && acceptableType(col.type))
                    retainedCols.push(col);
            });

            if ($common.isEmptyArray(retainedCols))
                for (idx = 0; idx < allColsLen; idx++) {
                    var col = allCols[idx];

                    if (acceptableType(col.type) && col != dfltCol) {
                        retainedCols.push(col);

                        break;
                    }
                }

            if ($common.isEmptyArray(retainedCols) && dfltCol && acceptableType(dfltCol.type))
                retainedCols.push(dfltCol);
        }

        return retainedCols;
    }

    var _processQueryResult = function (paragraph, refreshMode) {
        return function (res) {
            if (res.meta && !refreshMode) {
                paragraph.meta = [];

                paragraph.chartColumns = [];

                if (!$common.isDefined(paragraph.chartKeyCols))
                    paragraph.chartKeyCols = [];

                if (!$common.isDefined(paragraph.chartValCols ))
                    paragraph.chartValCols = [];

                if (res.meta.length <= 2) {
                    var _key = _.find(res.meta, {fieldName: '_KEY'});
                    var _val = _.find(res.meta, {fieldName: '_VAL'});

                    paragraph.disabledSystemColumns = (res.meta.length == 2 && _key && _val) ||
                        (res.meta.length == 1 && (_key || _val));
                }

                paragraph.columnFilter = _columnFilter(paragraph);

                paragraph.meta = res.meta;

                _rebuildColumns(paragraph);
            }

            paragraph.page = 1;

            paragraph.total = 0;

            paragraph.queryId = res.queryId;

            delete paragraph.errMsg;

            // Prepare explain results for display in table.
            if (paragraph.queryArgs.type == "EXPLAIN" && res.rows) {
                paragraph.rows = [];

                res.rows.forEach(function (row, i) {
                    var line = res.rows.length - 1 == i ? row[0] : row[0] + '\n';

                    line.replace(/\"/g, '').split('\n').forEach(function (line) {
                        paragraph.rows.push([line]);
                    });
                });
            }
            else
                paragraph.rows = res.rows;

            paragraph.gridOptions.api.setRowData(paragraph.rows);

            if (!refreshMode)
                setTimeout(function () { paragraph.gridOptions.api.sizeColumnsToFit(); });

            // Add results to history.
            chartHistory.push({tm: new Date(), rows: paragraph.rows});

            if (chartHistory.length > HISTORY_LENGTH)
                chartHistory.shift();

            _showLoading(paragraph, false);

            if (paragraph.result == 'none' || paragraph.queryArgs.type != "QUERY")
                paragraph.result = 'table';
            else if (paragraph.chart())
                _chartApplySettings(paragraph);
        }
    };

    var _executeRefresh = function (paragraph) {
        $http.post('/agent/query', paragraph.queryArgs)
            .success(_processQueryResult(paragraph, true))
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;
            });
    };

    var _showLoading = function (paragraph, enable) {
        if (paragraph.table())
            paragraph.gridOptions.api.showLoading(enable);

        paragraph.loading = enable;
    };

    $scope.execute = function (paragraph) {
        _saveNotebook();

        paragraph.queryArgs = { type: "QUERY", query: paragraph.query, pageSize: paragraph.pageSize, cacheName: paragraph.cacheName };

        _showLoading(paragraph, true);

        $http.post('/agent/query', paragraph.queryArgs)
            .success(function (res) {
                _processQueryResult(paragraph)(res);

                _tryStartRefresh(paragraph);
            })
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;

                _showLoading(paragraph, false);

                $scope.stopRefresh(paragraph);
            });

        paragraph.ace.focus();
    };

    $scope.explain = function (paragraph) {
        _saveNotebook();

        _cancelRefresh(paragraph);

        paragraph.queryArgs = { type: "EXPLAIN", query: 'EXPLAIN ' + paragraph.query, pageSize: paragraph.pageSize, cacheName: paragraph.cacheName };

        _showLoading(paragraph, true);

        $http.post('/agent/query', paragraph.queryArgs)
            .success(_processQueryResult(paragraph))
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;

                _showLoading(paragraph, false);
            });

        paragraph.ace.focus();
    };

    $scope.scan = function (paragraph) {
        _saveNotebook();

        _cancelRefresh(paragraph);

        paragraph.queryArgs = { type: "SCAN", pageSize: paragraph.pageSize, cacheName: paragraph.cacheName };

        _showLoading(paragraph, true);

        $http.post('/agent/scan', paragraph.queryArgs)
            .success(_processQueryResult(paragraph))
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;

                _showLoading(paragraph, false);
            });

        paragraph.ace.focus();
    };

    $scope.nextPage = function(paragraph) {
        _showLoading(paragraph, true);

        $http.post('/agent/query/fetch', {queryId: paragraph.queryId, pageSize: paragraph.pageSize, cacheName: paragraph.queryArgs.cacheName})
            .success(function (res) {
                paragraph.page++;

                paragraph.total += paragraph.rows.length;

                paragraph.rows = res.rows;

                paragraph.gridOptions.api.setRowData(res.rows);

                _showLoading(paragraph, false);

                if (res.last)
                    delete paragraph.queryId;
            })
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;

                _showLoading(paragraph, false);
            });
    };

    var _export = function(fileName, meta, rows) {
        var csvContent = '';

        if (meta) {
            csvContent += meta.map(function (col) {
                var res = [];

                if (col.schemaName)
                    res.push(col.schemaName);
                if (col.typeName)
                    res.push(col.typeName);

                res.push(col.fieldName);

                return res.join('.');
            }).join(',') + '\n';
        }

        rows.forEach(function (row) {
            if (Array.isArray(row)) {
                csvContent += row.map(function (elem) {
                    return elem ? JSON.stringify(elem) : '';
                }).join(',');
            }
            else {
                var first = true;

                meta.forEach(function (prop) {
                    if (first)
                        first = false;
                    else
                        csvContent += ',';

                    var elem = row[prop.fieldName];

                    csvContent += elem ? JSON.stringify(elem) : '';
                });
            }

            csvContent += '\n';
        });

        $common.download('application/octet-stream;charset=utf-8', fileName, escape(csvContent));
    };

    $scope.exportPage = function(paragraph) {
        _export(paragraph.name + '.csv', paragraph.meta, paragraph.rows);
    };

    $scope.exportAll = function(paragraph) {
        $http.post('/agent/query/getAll', {query: paragraph.query, cacheName: paragraph.cacheName})
            .success(function (item) {
                _export(paragraph.name + '-all.csv', item.meta, item.rows);
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.rateAsString = function (paragraph) {
        if (paragraph.rate && paragraph.rate.installed) {
            var idx = _.findIndex($scope.timeUnit, function (unit) {
                return unit.value == paragraph.rate.unit;
            });

            if (idx >= 0)
                return ' ' + paragraph.rate.value + $scope.timeUnit[idx].short;

            paragraph.rate.installed = false;
        }

        return '';
    };

    var _cancelRefresh = function (paragraph) {
        if (paragraph.rate && paragraph.rate.stopTime) {
            delete paragraph.queryArgs;

            paragraph.rate.installed = false;

            $interval.cancel(paragraph.rate.stopTime);

            delete paragraph.rate.stopTime;
        }
    };

    var _tryStopRefresh = function (paragraph) {
        if (paragraph.rate && paragraph.rate.stopTime) {
            $interval.cancel(paragraph.rate.stopTime);

            delete paragraph.rate.stopTime;
        }
    };

    var _tryStartRefresh = function (paragraph) {
        _tryStopRefresh(paragraph);

        if (paragraph.rate && paragraph.rate.installed && paragraph.queryArgs) {
            $scope.chartAcceptKeyColumn(paragraph, TIME_LINE);

            _executeRefresh(paragraph);

            var delay = paragraph.rate.value * paragraph.rate.unit;

            paragraph.rate.stopTime = $interval(_executeRefresh, delay, 0, false, paragraph);
        }
    };

    $scope.startRefresh = function (paragraph, value, unit) {
        paragraph.rate.value = value;
        paragraph.rate.unit = unit;
        paragraph.rate.installed = true;

        _tryStartRefresh(paragraph);
    };

    $scope.stopRefresh = function (paragraph) {
        paragraph.rate.installed = false;

        _tryStopRefresh(paragraph);
    };

    function _chartNumber(arr, idx, dflt) {
        if (arr && arr.length > idx && _.isNumber(arr[idx])) {
            return arr[idx];
        }

        return dflt;
    }

    function _chartLabel(arr, idx, dflt) {
        if (arr && arr.length > idx && _.isString(arr[idx]))
            return arr[idx];

        return dflt;
    }

    function _chartDatum(paragraph) {
        var datum = [];

        if (paragraph.chartColumnsConfigured()) {
            paragraph.chartValCols.forEach(function (valCol) {
                var index = 0;
                var values = [];

                if (paragraph.chartTimeLineEnabled()) {
                    if (paragraph.charts && paragraph.charts.length == 1)
                        datum = paragraph.charts[0].data;

                    var chartData = _.find(datum, {key: valCol.label});

                    if (chartData) {
                        var history = _.last(chartHistory);

                        chartData.values.push({
                            x: history.tm,
                            y: _chartNumber(history.rows[0], valCol.value, index++)
                        });

                        if (chartData.length > HISTORY_LENGTH)
                            chartData.shift();
                    }
                    else {
                        var tm = new Date();

                        values = _.map(chartHistory, function (history) {
                            return {
                                x: history.tm,
                                y: _chartNumber(history.rows[0], valCol.value, index++)
                            }
                        });

                        datum.push({key: valCol.label, values: values});
                    }
                }
                else {
                    values = _.map(paragraph.rows, function (row) {
                        return {
                            x: _chartNumber(row, paragraph.chartKeyCols[0].value, index),
                            xLbl: _chartLabel(row, paragraph.chartKeyCols[0].value, undefined),
                            y: _chartNumber(row, valCol.value, index++)
                        }
                    });

                    datum.push({key: valCol.label, values: values});
                }
            });
        }

        return datum;
    }

    function _pieChartDatum(paragraph) {
        var datum = [];

        if (paragraph.chartColumnsConfigured() && !paragraph.chartTimeLineEnabled()) {
            paragraph.chartValCols.forEach(function (valCol) {
                var index = 0;

                var values = _.map(paragraph.rows, function (row) {
                    return {
                        x: row[paragraph.chartKeyCols[0].value],
                        y: _chartNumber(row, valCol.value, index++)
                    }
                });

                datum.push({key: valCol.label, values: values});
            });
        }

        return datum;
    }

    $scope.paragraphTimeSpanVisible = function (paragraph) {
        return paragraph.timeLineSupported() && paragraph.chartTimeLineEnabled();
    };

    $scope.paragraphTimeLineSpan = function (paragraph) {
      if (paragraph && paragraph.timeLineSpan)
        return paragraph.timeLineSpan.toString();

        return '1';
    };

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
            }
        }
    }

    $scope.applyChartTimeFrame = function (paragraph) {
        _chartApplySettings(paragraph, true);
    };

    function _colLabel(col) {
        return col.label;
    }

    function _chartAxisLabel(cols, dflt) {
        return $common.isEmptyArray(cols) ? dflt : _.map(cols, _colLabel).join(', ');
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

    var _xAxisWithLabelFormat = function(values) {
        return function (d) {
            var dx = values[d];

            if (!dx)
                return d3.format(',.2f')(d);

            var lbl = values[d]['xLbl'];

            return lbl ? lbl : d3.format(',.2f')(d);
        }
    };

    function _barChart(paragraph) {
        var datum = _chartDatum(paragraph);

        if ($common.isEmptyArray(paragraph.charts)) {
            var options = {
                chart: {
                    type: 'multiBarChart',
                    height: 400,
                    margin: {left: 70},
                    duration: 0,
                    x: _xX,
                    y: _yY,
                    xAxis: {
                        axisLabel: _chartAxisLabel(paragraph.chartKeyCols, 'X'),
                        tickFormat: paragraph.chartTimeLineEnabled() ? _xAxisTimeFormat : _xAxisWithLabelFormat(datum[0].values),
                        showMaxMin: false
                    },
                    yAxis: {
                        axisLabel:  _chartAxisLabel(paragraph.chartValCols, 'Y'),
                        tickFormat: d3.format(',.2f')
                    },
                    showControls: true
                }
            };

            paragraph.charts = [{options: options, data: datum}];

            $timeout(function () {
                paragraph.charts[0].api.update();
            }, 100);
        }
        else
            $timeout(function () {
                if (paragraph.chartTimeLineEnabled())
                    paragraph.charts[0].api.update();
                else
                    paragraph.charts[0].api.updateWithData(datum);
            });
    }

    function _pieChart(paragraph) {
        var datum = _pieChartDatum(paragraph);

        if (datum.length == 0)
            datum = [{values: []}];

        paragraph.charts = _.map(datum, function (data) {
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
                        donutRatio: 0.35
                    },
                    title: {
                        enable: true,
                        text: data.key
                    }
                },
                data: data.values
            }
        });

        $timeout(function () {
            paragraph.charts[0].api.update();
        }, 100);
    }

    function _lineChart(paragraph) {
        var datum = _chartDatum(paragraph);

        if ($common.isEmptyArray(paragraph.charts)) {
            var options = {
                chart: {
                    type: 'lineChart',
                    height: 400,
                    margin: { left: 70 },
                    duration: 0,
                    x: _xX,
                    y: _yY,
                    xAxis: {
                        axisLabel: _chartAxisLabel(paragraph.chartKeyCols, 'X'),
                        tickFormat: paragraph.chartTimeLineEnabled() ? _xAxisTimeFormat : _xAxisWithLabelFormat(datum[0].values),
                        showMaxMin: false
                    },
                    yAxis: {
                        axisLabel:  _chartAxisLabel(paragraph.chartValCols, 'Y'),
                        tickFormat: d3.format(',.2f')
                    },
                    useInteractiveGuideline: true
                }
            };

            paragraph.charts = [{options: options, data: datum}];

            $timeout(function () {
                paragraph.charts[0].api.update();
            }, 100);
        }
        else
            $timeout(function () {
                if (paragraph.chartTimeLineEnabled())
                    paragraph.charts[0].api.update();
                else
                    paragraph.charts[0].api.updateWithData(datum);
            });
    }

    function _areaChart(paragraph) {
        var datum = _chartDatum(paragraph);

        if ($common.isEmptyArray(paragraph.charts)) {
            var options = {
                chart: {
                    type: 'stackedAreaChart',
                    height: 400,
                    margin: {left: 70},
                    duration: 0,
                    x: _xX,
                    y: _yY,
                    xAxis: {
                        axisLabel:  _chartAxisLabel(paragraph.chartKeyCols, 'X'),
                        tickFormat: paragraph.chartTimeLineEnabled() ? _xAxisTimeFormat : _xAxisWithLabelFormat(datum[0].values),
                        showMaxMin: false
                    },
                    yAxis: {
                        axisLabel:  _chartAxisLabel(paragraph.chartValCols, 'Y'),
                        tickFormat: d3.format(',.2f')
                    }
                }
            };

            paragraph.charts = [{options: options, data: datum}];

            $timeout(function () {
                paragraph.charts[0].api.update();
            }, 100);
        }
        else
            $timeout(function () {
                if (paragraph.chartTimeLineEnabled())
                    paragraph.charts[0].api.update();
                else
                    paragraph.charts[0].api.updateWithData(datum);
            });
    }

    $scope.actionAvailable = function (paragraph, needQuery) {
        return $scope.caches.length > 0 && paragraph.cacheName && (!needQuery || paragraph.query) && !paragraph.loading;
    };

    $scope.actionTooltip = function (paragraph, action, needQuery) {
        if ($scope.actionAvailable(paragraph, needQuery))
            return;

        if (paragraph.loading)
            return 'Wating for server response';

        return 'To ' + action + ' query select cache' + (needQuery ? ' and input query' : '');
    };

    $scope.clickableMetadata = function (node) {
        return node.type.slice(0, 5) != 'index';
    };

    $scope.dblclickMetadata = function (paragraph, node) {
        paragraph.ace.insert(node.name);
        var position = paragraph.ace.selection.getCursor();

        paragraph.query = paragraph.ace.getValue();

        setTimeout(function () {
            paragraph.ace.selection.moveCursorToPosition(position);

            paragraph.ace.focus();
        }, 1);
    };

    $scope.tryLoadMetadata = function (cache) {
        if (!cache.metadata) {
            $loading.start('loadingCacheMetadata');

            $http.post('/agent/cache/metadata', {cacheName: cache.name})
                .success(function (metadata) {
                    cache.metadata = _.sortBy(metadata, 'name');
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                })
                .finally(function() {
                    $loading.finish('loadingCacheMetadata');
                });
        }
    }
}]);
