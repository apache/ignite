

import angular from 'angular';

import component from './component';
import gridColumnCategories from '../pco-grid-column-categories/directive';

export default angular
    .module('ignite-console.page-configure-overview', [])
    .component('pageConfigureOverview', component)
    .directive('pcoGridColumnCategories', gridColumnCategories);
