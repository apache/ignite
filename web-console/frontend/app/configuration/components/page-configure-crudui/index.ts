

import angular from 'angular';
import component from './component';
import cluster from './components/page-configure-crudui-cluster';
import services from './components/page-configure-crudui-services';
import models from './components/page-configure-crudui-models';
import caches from './components/page-configure-crudui-caches';
import cacheEditForm from './components/cache-edit-form';
import clusterEditForm from './components/cluster-edit-form';
import modelEditForm from './components/model-edit-form';

export default angular
    .module('ignite-console.page-configure-crudui', [
        cluster.name,
        services.name,
        models.name,
        caches.name,
        modelEditForm.name,
        cacheEditForm.name,
        clusterEditForm.name
    ])
    .component('pageConfigureCrudUI', component);
