

import angular from 'angular';

import component from './component';
import gridColumnCategories from 'app/configuration/components/pco-grid-column-categories/directive';

export default angular
    .module('ignite-console.page-console-overview', [])
    .component('pageConsoleOverview', component)
    .directive('pcoGridColumnCategories', gridColumnCategories);
