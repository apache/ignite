

import angular from 'angular';
import component from './component';
import cluster from './components/page-configure-advanced-cluster';
import models from './components/page-configure-advanced-models';
import caches from './components/page-configure-advanced-caches';
import cacheEditForm from './components/cache-edit-form';
import clusterEditForm from './components/cluster-edit-form';
import modelEditForm from './components/model-edit-form';

export default angular
    .module('ignite-console.page-configure-advanced', [
        cluster.name,
        models.name,
        caches.name,
        modelEditForm.name,
        cacheEditForm.name,
        clusterEditForm.name
    ])
    .component('pageConfigureAdvanced', component);
