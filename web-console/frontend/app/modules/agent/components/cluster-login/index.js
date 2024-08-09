

import angular from 'angular';
import {component} from './component';
import service from './service';

export default angular
    .module('ignite-console.agent.cluster-login', [])
    .service('ClusterLoginService', service)
    .component('clusterLogin', component);
