

import angular from 'angular';
import component from './component';
import cluster from './components/page-configure-advanced-cluster';
import models from './components/page-configure-advanced-models';
import caches from './components/page-configure-advanced-caches';
import cacheMericForm from './components/cache-meric-form';
import modelMericForm from './components/model-meric-form';
import clusterControlOverview from './components/cluster-control-overview';

export default angular
    .module('ignite-console.page-console-advanced', [
        cluster.name,
        models.name,
        caches.name,
        modelMericForm.name,
        cacheMericForm.name,       
        clusterControlOverview.name
    ])
    .component('pageConsoleAdvanced', component);
