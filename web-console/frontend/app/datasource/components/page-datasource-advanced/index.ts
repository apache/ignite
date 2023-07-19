

import angular from 'angular';
import component from './component';


import datasourceEditForm from './components/datasource-edit-form';

export default angular
    .module('ignite-console.page-datasource-advanced', [ 
        datasourceEditForm.name
    ])
    .component('pageDatasourceAdvanced', component);

