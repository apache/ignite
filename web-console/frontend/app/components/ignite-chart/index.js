

import angular from 'angular';

import chartNoData from './components/chart-no-data';
import IgniteChartCmp from './component';
import './style.scss';

export default angular
    .module('ignite-console.ignite-chart', [chartNoData.name])
    .component('igniteChart', IgniteChartCmp);
