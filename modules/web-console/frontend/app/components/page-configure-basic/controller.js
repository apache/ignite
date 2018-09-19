/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Observable} from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import naturalCompare from 'natural-compare-lite';
import {
    changeItem,
    removeClusterItems,
    basicSave,
    basicSaveAndDownload
} from 'app/components/page-configure/store/actionCreators';

import {Confirm} from 'app/services/Confirm.service';
import ConfigureState from 'app/components/page-configure/services/ConfigureState';
import ConfigSelectors from 'app/components/page-configure/store/selectors';
import Caches from 'app/services/Caches';
import Clusters from 'app/services/Clusters';
import IgniteVersion from 'app/services/Version.service';
import {default as ConfigChangesGuard} from 'app/components/page-configure/services/ConfigChangesGuard';

export default class PageConfigureBasicController {
    /** @type {ng.IFormController} */
    form;

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', 'ConfigSelectors', 'Clusters', 'Caches', 'IgniteVersion', '$element', 'ConfigChangesGuard', 'IgniteFormUtils', '$scope'
    ];

    /**
     * @param {Confirm} Confirm
     * @param {uirouter.UIRouter} $uiRouter
     * @param {ConfigureState} ConfigureState
     * @param {ConfigSelectors} ConfigSelectors
     * @param {Clusters} Clusters
     * @param {Caches} Caches
     * @param {IgniteVersion} IgniteVersion
     * @param {JQLite} $element
     * @param {ConfigChangesGuard} ConfigChangesGuard
     * @param {object} IgniteFormUtils
     * @param {ng.IScope} $scope
     */
    constructor(Confirm, $uiRouter, ConfigureState, ConfigSelectors, Clusters, Caches, IgniteVersion, $element, ConfigChangesGuard, IgniteFormUtils, $scope) {
        Object.assign(this, {IgniteFormUtils});
        this.ConfigChangesGuard = ConfigChangesGuard;
        this.$uiRouter = $uiRouter;
        this.$scope = $scope;
        this.$element = $element;
        this.Caches = Caches;
        this.Clusters = Clusters;
        this.Confirm = Confirm;
        this.ConfigureState = ConfigureState;
        this.ConfigSelectors = ConfigSelectors;
        this.IgniteVersion = IgniteVersion;
    }

    $onDestroy() {
        this.subscription.unsubscribe();
        if (this.onBeforeTransition) this.onBeforeTransition();
        this.$element = null;
    }

    $postLink() {
        this.$element.addClass('panel--ignite');
    }

    _uiCanExit($transition$) {
        if ($transition$.options().custom.justIDUpdate)
            return true;

        $transition$.onSuccess({}, () => this.reset());

        return Observable.forkJoin(
            this.ConfigureState.state$.pluck('edit', 'changes').take(1),
            this.clusterID$.switchMap((id) => this.ConfigureState.state$.let(this.ConfigSelectors.selectClusterShortCaches(id))).take(1),
            this.shortCaches$.take(1)
        ).toPromise()
        .then(([changes, originalShortCaches, currentCaches]) => {
            return this.ConfigChangesGuard.guard(
                {
                    cluster: this.Clusters.normalize(this.originalCluster),
                    caches: originalShortCaches.map(this.Caches.normalize)
                },
                {
                    cluster: {...this.Clusters.normalize(this.clonedCluster), caches: changes.caches.ids},
                    caches: currentCaches.map(this.Caches.normalize)
                }
            );
        });
    }

    $onInit() {
        this.onBeforeTransition = this.$uiRouter.transitionService.onBefore({}, (t) => this._uiCanExit(t));

        this.memorySizeInputVisible$ = this.IgniteVersion.currentSbj
            .map((version) => this.IgniteVersion.since(version.ignite, '2.0.0'));

        const clusterID$ = this.$uiRouter.globals.params$.take(1).pluck('clusterID').filter((v) => v).take(1);
        this.clusterID$ = clusterID$;

        this.isNew$ = this.$uiRouter.globals.params$.pluck('clusterID').map((id) => id === 'new');
        this.shortCaches$ = this.ConfigureState.state$.let(this.ConfigSelectors.selectCurrentShortCaches);
        this.shortClusters$ = this.ConfigureState.state$.let(this.ConfigSelectors.selectShortClustersValue());
        this.originalCluster$ = clusterID$.distinctUntilChanged().switchMap((id) => {
            return this.ConfigureState.state$.let(this.ConfigSelectors.selectClusterToEdit(id));
        }).distinctUntilChanged().publishReplay(1).refCount();

        this.subscription = Observable.merge(
            this.shortCaches$.map((caches) => caches.sort((a, b) => naturalCompare(a.name, b.name))).do((v) => this.shortCaches = v),
            this.shortClusters$.do((v) => this.shortClusters = v),
            this.originalCluster$.do((v) => {
                this.originalCluster = v;
                // clonedCluster should be set only when particular cluster edit starts.
                // 
                // Stored cluster changes should not propagate to clonedCluster because it's assumed
                // that last saved copy has same shape to what's already loaded. If stored cluster would overwrite
                // clonedCluster every time, then data rollback on server errors would undo all changes
                // made by user and we don't want that. Advanced configuration forms do the same too.
                if (get(v, '_id') !== get(this.clonedCluster, '_id')) this.clonedCluster = cloneDeep(v);
                this.defaultMemoryPolicy = this.Clusters.getDefaultClusterMemoryPolicy(this.clonedCluster);
            })
        ).subscribe();

        this.formActionsMenu = [
            {
                text: 'Save and Download',
                click: () => this.save(true),
                icon: 'download'
            },
            {
                text: 'Save',
                click: () => this.save(),
                icon: 'checkmark'
            }
        ];

        this.cachesColDefs = [
            {name: 'Name:', cellClass: 'pc-form-grid-col-10'},
            {name: 'Mode:', cellClass: 'pc-form-grid-col-10'},
            {name: 'Atomicity:', cellClass: 'pc-form-grid-col-10', tip: `
                Atomicity:
                <ul>
                    <li>ATOMIC - in this mode distributed transactions and distributed locking are not supported</li>
                    <li>TRANSACTIONAL - in this mode specified fully ACID-compliant transactional cache behavior</li>
                </ul>
            `},
            {name: 'Backups:', cellClass: 'pc-form-grid-col-10', tip: `
                Number of nodes used to back up single partition for partitioned cache
            `}
        ];
    }

    addCache() {
        this.ConfigureState.dispatchAction({type: 'ADD_CACHE_TO_EDIT'});
    }

    removeCache(cache) {
        this.ConfigureState.dispatchAction(
            removeClusterItems(this.$uiRouter.globals.params.clusterID, 'caches', [cache._id], false, false)
        );
    }

    changeCache(cache) {
        return this.ConfigureState.dispatchAction(changeItem('caches', cache));
    }

    save(download = false) {
        if (this.form.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.form, this.$scope);

        this.ConfigureState.dispatchAction((download ? basicSaveAndDownload : basicSave)(cloneDeep(this.clonedCluster)));
    }

    reset() {
        this.clonedCluster = cloneDeep(this.originalCluster);
        this.ConfigureState.dispatchAction({type: 'RESET_EDIT_CHANGES'});
    }

    confirmAndReset() {
        return this.Confirm.confirm('Are you sure you want to undo all changes for current cluster?')
            .then(() => this.reset())
            .catch(() => {});
    }
}
