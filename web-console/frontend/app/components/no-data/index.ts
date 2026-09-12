

import angular from 'angular';

import noDataCmp from './component';
import './style.scss';

export default angular
    .module('ignite-console.no-data', [])
    .component('noData', noDataCmp);
