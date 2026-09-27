

import angular from 'angular';

import component from './component';
import taskFlowEditForm from './components/taskflow-edit-form';

export default angular
    .module('ignite-console.cluster-task-flow-overview', [taskFlowEditForm.name])    
    .component('clusterTaskFlowOverview', component);
