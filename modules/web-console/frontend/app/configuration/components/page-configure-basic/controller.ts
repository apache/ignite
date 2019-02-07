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

import {forkJoin, merge} from 'rxjs';
import {map, tap, pluck, take, filter, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import cloneDeep from 'lodash/cloneDeep';
import get from 'lodash/get';
import naturalCompare from 'natural-compare-lite';
import {
    changeItem,
    removeClusterItems,
    basicSave,
    basicSaveAndDownload
} from '../../store/actionCreators';

import {Confirm} from 'app/services/Confirm.service';
import ConfigureState from '../../services/ConfigureState';
import ConfigSelectors from '../../store/selectors';
import Caches from '../../services/Caches';
import Clusters from '../../services/Clusters';
import IgniteVersion from 'app/services/Version.service';
import {default as ConfigChangesGuard} from '../../services/ConfigChangesGuard';
import {UIRouter} from '@uirouter/angularjs';
import FormUtils from 'app/services/FormUtils.service';

export default class PageConfigureBasicController {
    form: ng.IFormController;

    static $inject = [
        'Confirm', '$uiRouter', 'ConfigureState', 'ConfigSelectors', 'Clusters', 'Caches', 'IgniteVersion', '$element', 'ConfigChangesGuard', 'IgniteFormUtils', '$scope'
    ];

    constructor(
        private Confirm: Confirm,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors,
        private Clusters: Clusters,
        private Caches: Caches,
        private IgniteVersion: IgniteVersion,
        private $element: JQLite,
        private ConfigChangesGuard: ReturnType<typeof ConfigChangesGuard>,
        private IgniteFormUtils: ReturnType<typeof FormUtils>,
        private $scope: ng.IScope
    ) {}

    $onDestroy() {
        this.subscription.unsubscribe();
        if (this.onBeforeTransition) this.onBeforeTransition();
        this.$element = null;
    }

    $postLink() {
        this.$element.addClass('panel--ignite');
    }

    _uiCanExit($transition$) {
        const options = $transition$.options();

        if (options.custom.justIDUpdate || options.redirectedFrom)
            return true;

        $transition$.onSuccess({}, () => this.reset());

        return forkJoin(
            this.ConfigureState.state$.pipe(pluck('edit', 'changes'), take(1)),
            this.clusterID$.pipe(
                switchMap((id) => this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterShortCaches(id))),
                take(1)
            ),
            this.shortCaches$.pipe(take(1))
        )
            .toPromise()
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

        this.memorySizeInputVisible$ = this.IgniteVersion.currentSbj.pipe(
            map((version) => this.IgniteVersion.since(version.ignite, '2.0.0'))
        );

        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('clusterID'),
            filter((v) => v),
            take(1)
        );
        this.clusterID$ = clusterID$;

        this.isNew$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'), map((id) => id === 'new'));
        this.shortCaches$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCurrentShortCaches);
        this.shortClusters$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectShortClustersValue());
        this.originalCluster$ = clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );

        this.subscription = merge(
            this.shortCaches$.pipe(
                map((caches) => caches.sort((a, b) => naturalCompare(a.name, b.name))),
                tap((v) => this.shortCaches = v)
            ),
            this.shortClusters$.pipe(tap((v) => this.shortClusters = v)),
            this.originalCluster$.pipe(tap((v) => {
                this.originalCluster = v;
                // clonedCluster should be set only when particular cluster edit starts.
                // 
                // Stored cluster changes should not propagate to clonedCluster because it's assumed
                // that last saved copy has same shape to what's already loaded. If stored cluster would overwrite
                // clonedCluster every time, then data rollback on server errors would undo all changes
                // made by user and we don't want that. Advanced configuration forms do the same too.
                if (get(v, '_id') !== get(this.clonedCluster, '_id')) this.clonedCluster = cloneDeep(v);
                this.defaultMemoryPolicy = this.Clusters.getDefaultClusterMemoryPolicy(this.clonedCluster);
            }))
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
            {name: 'Atomicity:', cellClass: 'pc-form-grid-col-20', tip: `
                Atomicity:
                <ul>
                    <li>ATOMIC - in this mode distributed transactions and distributed locking are not supported</li>
                    <li>TRANSACTIONAL - in this mode specified fully ACID-compliant transactional cache behavior</li>
                    <li>TRANSACTIONAL_SNAPSHOT - in this mode specified fully ACID-compliant transactional cache behavior for both key-value API and SQL transactions</li>
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
