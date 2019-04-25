/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export default class IgniteChartSeriesSelectorController {
    constructor() {
        this.charts = [];
        this.selectedCharts = [];
    }

    $onChanges(changes) {
        if (changes && 'chartApi' in changes && changes.chartApi.currentValue) {
            this.applyValues();
            this.setSelectedCharts();
        }
    }

    applyValues() {
        this.charts = this._makeMenu();
        this.selectedCharts = this.charts.filter((chart) => !chart.hidden).map(({ key }) => key);
    }

    setSelectedCharts() {
        const selectedDataset = ({ label }) => this.selectedCharts.includes(label);

        this.chartApi.config.data.datasets
            .forEach((dataset) => {
                dataset.hidden = true;

                if (!selectedDataset(dataset))
                    return;

                dataset.hidden = false;
            });

        this.chartApi.update();
    }

    _makeMenu() {
        const labels = this.chartApi.config.datasetLegendMapping;

        return Object.keys(this.chartApi.config.datasetLegendMapping).map((key) => {
            return {
                key,
                label: labels[key].name || labels[key],
                hidden: labels[key].hidden
            };
        });
    }
}
