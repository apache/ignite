

import _ from 'lodash';
import moment from 'moment';
import {ChartConfiguration} from 'chart.js';

type IgniteChartDataPoint = {x: number, y: {[key: string]: number}}
type RangeOption = {label: string, value: number}

const RANGE_RATE_PRESET: RangeOption[] = [
    {label: 'igniteChartComponent.ranges.1', value: 1},
    {label: 'igniteChartComponent.ranges.5', value: 5},
    {label: 'igniteChartComponent.ranges.10', value: 10},
    {label: 'igniteChartComponent.ranges.15', value: 15},
    {label: 'igniteChartComponent.ranges.30', value: 30}
];

/**
 * Determines what label format was chosen by determineLabelFormat function
 * in Chart.js streaming plugin.
 */
const inferLabelFormat = (label: string) => {
    if (label.match(/\.\d{3} (am|pm)$/)) return 'MMM D, YYYY h:mm:ss.SSS a';
    if (label.match(/:\d{1,2} (am|pm)$/)) return 'MMM D, YYYY h:mm:ss a';
    if (label.match(/ \d{4}$/)) return 'MMM D, YYYY';
};

export class IgniteChartController {
    chartOptions: ChartConfiguration;
    chartTitle: string;
    chartDataPoint: IgniteChartDataPoint;
    chartHistory: IgniteChartDataPoint[];
    newPoints = [];

    datePipe = this.$filter('date');
    ranges = RANGE_RATE_PRESET;
    currentRange = this.ranges[0];
    maxRangeInMilliseconds = RANGE_RATE_PRESET[RANGE_RATE_PRESET.length - 1].value * 60 * 1000;
    ctx = (this.$element.find('canvas')[0] as HTMLCanvasElement).getContext('2d');
    localHistory = [];
    updateIsBusy = false;

    chart?: Chart;
    config?: ChartConfiguration;
    refreshRate: number;
    xRangeUpdateInProgress?: boolean;
    chartColors: string[];

    static $inject = ['$element', 'IgniteChartColors', '$filter', '$translate'];

    constructor(
        private $element: JQLite,
        private IgniteChartColors: Array<string>,
        private $filter: ng.IFilterService,
        private $translate: ng.translate.ITranslateService
    ) {}

    $onDestroy() {
        if (this.chart)
            this.chart.destroy();

        this.$element = this.ctx = this.chart = null;
    }

    $onInit() {
        this.chartColors = _.get(this.chartOptions, 'chartColors', this.IgniteChartColors);
    }

    _refresh() {
        this.onRefresh();
        this.rerenderChart();
    }

    async $onChanges(changes: {chartOptions: ng.IChangesObject<import('chart.js').ChartConfiguration>, chartTitle: ng.IChangesObject<string>, chartDataPoint: ng.IChangesObject<IgniteChartDataPoint>, chartHistory: ng.IChangesObject<Array<IgniteChartDataPoint>>}) {
        if (this.chart && _.get(changes, 'refreshRate.currentValue'))
            this.onRefreshRateChanged(_.get(changes, 'refreshRate.currentValue'));

        if ((changes.chartDataPoint && _.isNil(changes.chartDataPoint.currentValue)) ||
            (changes.chartHistory && _.isEmpty(changes.chartHistory.currentValue))) {
            this.clearDatasets();
            this.localHistory = [];

            return;
        }

        if (changes.chartHistory && changes.chartHistory.currentValue && changes.chartHistory.currentValue.length !== changes.chartHistory.previousValue.length) {
            if (!this.chart)
                await this.initChart();

            this.clearDatasets();
            this.localHistory = [...changes.chartHistory.currentValue];

            this.newPoints.splice(0, this.newPoints.length, ...changes.chartHistory.currentValue);

            this._refresh();

            return;
        }

        if (this.chartDataPoint && changes.chartDataPoint) {
            if (!this.chart)
                this.initChart();

            this.newPoints.push(this.chartDataPoint);
            this.localHistory.push(this.chartDataPoint);

            this._refresh();
        }
    }

    async initChart() {
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
                            return tooltipItem[0].xLabel = moment(tooltipItem[0].xLabel, inferLabelFormat(tooltipItem[0].xLabel)).format('HH:mm:ss');
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
                        // Temporary workaround before https://github.com/nagix/chartjs-plugin-streaming/issues/53 resolved.
                        // ttl: this.maxRangeInMilliseconds,
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

    appendChartPoint(dataPoint: IgniteChartDataPoint) {
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

        // Temporary workaround before https://github.com/nagix/chartjs-plugin-streaming/issues/53 resolved.
        this.pruneHistory();
    }

    // Temporary workaround before https://github.com/nagix/chartjs-plugin-streaming/issues/53 resolved.
    pruneHistory() {
        if (!this.xRangeUpdateInProgress) {
            const currenTime = Date.now();

            while (currenTime - this.localHistory[0].x > this.maxRangeInMilliseconds)
                this.localHistory.shift();

            this.config.data.datasets.forEach((dataset) => {
                while (currenTime - dataset.data[0].x > this.maxRangeInMilliseconds)
                    dataset.data.shift();
            });
        }
    }

    /**
     * Checks if a key of dataset can be added to chart or should be ignored.
     */
    checkDatasetCanBeAdded(dataPointKey: string): boolean {
        // If datasetLegendMapping is empty all keys are allowed.
        if (!this.config.datasetLegendMapping)
            return true;

        return Object.keys(this.config.datasetLegendMapping).includes(dataPointKey);
    }

    clearDatasets() {
        if (!_.isNil(this.config))
            this.config.data.datasets.forEach((dataset) => dataset.data = []);
    }

    addDataset(datasetName: string) {
        if (this.findDatasetIndex(datasetName) >= 0)
            throw new Error(this.$translate.instant('igniteChartComponent.duplicateDatasetErrorMessage', {datasetName}));
        else {
            const datasetIsHidden = _.isNil(this.config.datasetLegendMapping[datasetName].hidden)
                ? false
                : this.config.datasetLegendMapping[datasetName].hidden;

            this.config.data.datasets.push({ label: datasetName, data: [], hidden: datasetIsHidden });
        }
    }

    findDatasetIndex(searchedDatasetLabel: string) {
        return this.config.data.datasets.findIndex((dataset) => dataset.label === searchedDatasetLabel);
    }

    changeXRange(range: RangeOption) {
        if (this.chart) {
            this.xRangeUpdateInProgress = true;

            this.chart.config.options.plugins.streaming.duration = range.value * 60 * 1000;

            this.clearDatasets();
            this.newPoints.splice(0, this.newPoints.length, ...this.localHistory);

            this.onRefresh();
            this.rerenderChart();

            this.xRangeUpdateInProgress = false;
        }
    }

    onRefreshRateChanged(refreshRate: number) {
        this.chart.config.options.plugins.streaming.frameRate = 1000 / refreshRate;
        this.chart.config.options.plugins.streaming.refresh = refreshRate;
        this.rerenderChart();
    }

    rerenderChart() {
        if (this.chart)
            this.chart.update();
    }
}
