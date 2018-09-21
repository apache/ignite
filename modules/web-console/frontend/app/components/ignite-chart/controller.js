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

import _ from 'lodash';

/**
 * @typedef {{x: number, y: {[key: string]: number}}} IgniteChartDataPoint
 */

const RANGE_RATE_PRESET = [{
    label: '1 min',
    value: 1
}, {
    label: '5 min',
    value: 5
}, {
    label: '10 min',
    value: 10
}, {
    label: '15 min',
    value: 15
}, {
    label: '30 min',
    value: 30
}];

export class IgniteChartController {
    /** @type {import('chart.js').ChartConfiguration} */
    chartOptions;
    /** @type {string} */
    chartTitle;
    /** @type {IgniteChartDataPoint} */
    chartDataPoint;
    /** @type {Array<IgniteChartDataPoint>} */
    chartHistory;
    newPoints = [];

    static $inject = ['$element', 'IgniteChartColors', '$filter'];

    /**
     * @param {JQLite} $element
     * @param {ng.IScope} $scope
     * @param {ng.IFilterService} $filter
     */
    constructor($element, IgniteChartColors, $filter) {
        this.$element = $element;
        this.IgniteChartColors = IgniteChartColors;

        this.datePipe = $filter('date');
        this.ranges = RANGE_RATE_PRESET;
        this.currentRange = this.ranges[0];
        this.maxRangeInMilliseconds = RANGE_RATE_PRESET[RANGE_RATE_PRESET.length - 1].value * 60 * 1000;
        this.ctx = this.$element.find('canvas')[0].getContext('2d');

        this.localHistory = [];
        this.updateIsBusy = false;
    }

    $onDestroy() {
        if (this.chart) this.chart.destroy();
        this.$element = this.ctx = this.chart = null;
    }

    $onInit() {
        this.chartColors = _.get(this.chartOptions, 'chartColors', this.IgniteChartColors);
    }

    /**
     * @param {{chartOptions: ng.IChangesObject<import('chart.js').ChartConfiguration>, chartTitle: ng.IChangesObject<string>, chartDataPoint: ng.IChangesObject<IgniteChartDataPoint>, chartHistory: ng.IChangesObject<Array<IgniteChartDataPoint>>}} changes
     */
    async $onChanges(changes) {
        if (this.chart && _.get(changes, 'refreshRate.currentValue'))
            this.onRefreshRateChanged(_.get(changes, 'refreshRate.currentValue'));

        // TODO: Investigate other signaling for resetting component state.
        if (changes.chartDataPoint && _.isNil(changes.chartDataPoint.currentValue)) {
            this.clearDatasets();
            return;
        }

        if (changes.chartHistory && changes.chartHistory.currentValue && changes.chartHistory.currentValue.length !== changes.chartHistory.previousValue.length) {
            if (!this.chart)
                await this.initChart();

            this.clearDatasets();
            this.localHistory = [...changes.chartHistory.currentValue];

            this.newPoints.splice(0, this.newPoints.length, ...changes.chartHistory.currentValue);

            this.onRefresh();
            this.rerenderChart();
            return;
        }

        if (this.chartDataPoint && changes.chartDataPoint) {
            if (!this.chart)
                this.initChart();

            this.newPoints.push(this.chartDataPoint);
            this.localHistory.push(this.chartDataPoint);
        }
    }

    async initChart() {
        /** @type {import('chart.js').ChartConfiguration} */
        this.config = {
            type: 'LineWithVerticalCursor',
            data: {
                datasets: []
            },
            options: {
                elements: {
                    line: {
                        tension: 0
                    },
                    point: {
                        radius: 2,
                        pointStyle: 'rectRounded'
                    }
                },
                animation: {
                    duration: 0 // general animation time
                },
                hover: {
                    animationDuration: 0 // duration of animations when hovering an item
                },
                responsiveAnimationDuration: 0, // animation duration after a resize
                maintainAspectRatio: false,
                responsive: true,
                legend: {
                    display: false
                },
                scales: {
                    xAxes: [{
                        type: 'realtime',
                        display: true,
                        time: {
                            displayFormats: {
                                second: 'HH:mm:ss',
                                minute: 'HH:mm:ss',
                                hour: 'HH:mm:ss'
                            }
                        },
                        ticks: {
                            maxRotation: 0,
                            minRotation: 0
                        }
                    }],
                    yAxes: [{
                        type: 'linear',
                        display: true,
                        ticks: {
                            min: 0,
                            beginAtZero: true,
                            maxTicksLimit: 4,
                            callback: (value, index, labels) => {
                                if (value === 0)
                                    return 0;

                                if (_.max(labels) <= 4000 && value <= 4000)
                                    return value;

                                if (_.max(labels) <= 1000000 && value <= 1000000)
                                    return `${value / 1000}K`;

                                if ((_.max(labels) <= 4000000 && value >= 500000) || (_.max(labels) > 4000000))
                                    return `${value / 1000000}M`;

                                return value;
                            }
                        }
                    }]
                },
                tooltips: {
                    mode: 'index',
                    position: 'yCenter',
                    intersect: false,
                    yAlign: 'center',
                    xPadding: 20,
                    yPadding: 20,
                    bodyFontSize: 13,
                    callbacks: {
                        title: (tooltipItem) => {
                            return tooltipItem[0].xLabel.slice(0, -7);
                        },
                        label: (tooltipItem, data) => {
                            const label = data.datasets[tooltipItem.datasetIndex].label || '';

                            return `${_.startCase(label)}: ${tooltipItem.yLabel} per sec`;
                        },
                        labelColor: (tooltipItem) => {
                            return {
                                borderColor: 'rgba(255,255,255,0.5)',
                                borderWidth: 0,
                                boxShadow: 'none',
                                backgroundColor: this.chartColors[tooltipItem.datasetIndex]
                            };
                        }
                    }
                },
                plugins: {
                    streaming: {
                        duration: this.currentRange.value * 1000 * 60,
                        frameRate: 1000 / this.refreshRate || 1 / 3,
                        refresh: this.refreshRate || 3000,
                        ttl: this.maxRangeInMilliseconds,
                        onRefresh: () => {
                            this.onRefresh();
                        }
                    }
                }
            }
        };

        this.config = _.merge(this.config, this.chartOptions);

        const chartModule = await import('chart.js');
        const Chart = chartModule.default;

        Chart.Tooltip.positioners.yCenter = (elements) => {
            const chartHeight = elements[0]._chart.height;
            const tooltipHeight = 60;

            return {x: elements[0].getCenterPoint().x, y: Math.floor(chartHeight / 2) - Math.floor(tooltipHeight / 2) };
        };


        // Drawing vertical cursor
        Chart.defaults.LineWithVerticalCursor = Chart.defaults.line;
        Chart.controllers.LineWithVerticalCursor = Chart.controllers.line.extend({
            draw(ease) {
                Chart.controllers.line.prototype.draw.call(this, ease);

                if (this.chart.tooltip._active && this.chart.tooltip._active.length) {
                    const activePoint = this.chart.tooltip._active[0];
                    const ctx = this.chart.ctx;
                    const x = activePoint.tooltipPosition().x;
                    const topY = this.chart.scales['y-axis-0'].top;
                    const bottomY = this.chart.scales['y-axis-0'].bottom;

                    // draw line
                    ctx.save();
                    ctx.beginPath();
                    ctx.moveTo(x, topY);
                    ctx.lineTo(x, bottomY);
                    ctx.lineWidth = 0.5;
                    ctx.strokeStyle = '#0080ff';
                    ctx.stroke();
                    ctx.restore();
                }
            }
        });

        await import('chartjs-plugin-streaming');

        this.chart = new Chart(this.ctx, this.config);
        this.changeXRange(this.currentRange);
    }

    onRefresh() {
        this.newPoints.forEach((point) => {
            this.appendChartPoint(point);
        });

        this.newPoints.splice(0, this.newPoints.length);
    }

    /**
     * @param {IgniteChartDataPoint} dataPoint
     */
    appendChartPoint(dataPoint) {
        Object.keys(dataPoint.y).forEach((key) => {
            if (this.checkDatasetCanBeAdded(key)) {
                let datasetIndex = this.findDatasetIndex(key);

                if (datasetIndex < 0) {
                    datasetIndex = this.config.data.datasets.length;
                    this.addDataset(key);
                }

                this.config.data.datasets[datasetIndex].data.push({x: dataPoint.x, y: dataPoint.y[key]});
                this.config.data.datasets[datasetIndex].borderColor = this.chartColors[datasetIndex];
                this.config.data.datasets[datasetIndex].borderWidth = 2;
                this.config.data.datasets[datasetIndex].fill = false;
            }
        });
    }

    /**
     * Checks if a key of dataset can be added to chart or should be ignored.
     * @param dataPointKey {String}
     * @return {Boolean}
     */
    checkDatasetCanBeAdded(dataPointKey) {
        // If datasetLegendMapping is empty all keys are allowed.
        if (!this.config.datasetLegendMapping)
            return true;

        return Object.keys(this.config.datasetLegendMapping).includes(dataPointKey);
    }

    clearDatasets() {
        if (!_.isNil(this.config))
            this.config.data.datasets.forEach((dataset) => dataset.data = []);
    }

    addDataset(datasetName) {
        if (this.findDatasetIndex(datasetName) >= 0)
            throw new Error(`Dataset with name ${datasetName} is already in chart`);
        else
            this.config.data.datasets.push({ label: datasetName, data: [], hidden: true });
    }

    findDatasetIndex(searchedDatasetLabel) {
        return this.config.data.datasets.findIndex((dataset) => dataset.label === searchedDatasetLabel);
    }

    changeXRange(range) {
        if (this.chart) {
            const deltaInMilliSeconds = range.value * 60 * 1000;
            this.chart.config.options.plugins.streaming.duration = deltaInMilliSeconds;

            this.clearDatasets();
            this.newPoints.splice(0, this.newPoints.length, ...this.localHistory);

            this.onRefresh();
            this.rerenderChart();
        }
    }

    onRefreshRateChanged(refreshRate) {
        this.chart.config.options.plugins.streaming.frameRate = 1000 / refreshRate;
        this.chart.config.options.plugins.streaming.refresh = refreshRate;
        this.rerenderChart();
    }

    rerenderChart() {
        this.chart.update();
    }
}
