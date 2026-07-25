

import angular from 'angular';
import component from './component';
import clusterTaskFlowOverview from '../cluster-taskflow-overview';

export default angular
    .module('ignite-console.page-console-advanced.caches', [clusterTaskFlowOverview.name])
    .component(component.name, component);
