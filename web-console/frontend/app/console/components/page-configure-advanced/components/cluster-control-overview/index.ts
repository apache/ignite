

import angular from 'angular';

import component from './component';
import commandCallForm from './components/command-call-form';

export default angular
    .module('ignite-console.cluster-control-overview', [commandCallForm.name])    
    .component('clusterControlOverview', component);
