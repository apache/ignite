/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import angular from 'angular';
import component from './component';
import cluster from './components/page-configure-advanced-cluster';
import models from './components/page-configure-advanced-models';
import caches from './components/page-configure-advanced-caches';
import igfs from './components/page-configure-advanced-igfs';
import cacheEditForm from './components/cache-edit-form';
import clusterEditForm from './components/cluster-edit-form';
import igfsEditForm from './components/igfs-edit-form';
import modelEditForm from './components/model-edit-form';

export default angular
    .module('ignite-console.page-configure-advanced', [
        cluster.name,
        models.name,
        caches.name,
        igfs.name,
        igfsEditForm.name,
        modelEditForm.name,
        cacheEditForm.name,
        clusterEditForm.name
    ])
    .component('pageConfigureAdvanced', component);
