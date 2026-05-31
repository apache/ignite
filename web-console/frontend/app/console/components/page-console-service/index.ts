

import angular from 'angular';

import component from './component';
import serviceCallForm from './components/service-call-form';

export default angular
    .module('ignite-console.page-console-service', [serviceCallForm.name])    
    .component('pageConsoleService', component);
