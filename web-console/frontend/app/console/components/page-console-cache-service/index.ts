

import angular from 'angular';

import component from './component';
import serviceCallForm from './components/cache-service-call-form';

export default angular
    .module('ignite-console.page-console-cache-service', [serviceCallForm.name])    
    .component('pageConsoleCacheService', component);
