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
import 'rxjs/add/operator/ignoreElements';
import 'rxjs/add/operator/let';
import 'rxjs/add/operator/exhaustMap';
import 'rxjs/add/operator/switchMap';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/pluck';
import 'rxjs/add/operator/withLatestFrom';
import 'rxjs/add/operator/merge';
import 'rxjs/add/operator/take';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/zip';
import {merge} from 'rxjs/observable/merge';
import {empty} from 'rxjs/observable/empty';
import {of} from 'rxjs/observable/of';
import {from} from 'rxjs/observable/from';
import {fromPromise} from 'rxjs/observable/fromPromise';
import uniq from 'lodash/uniq';
import {uniqueName} from 'app/utils/uniqueName';
import {defaultNames} from '../defaultNames';

import {
    cachesActionTypes,
    clustersActionTypes,
    igfssActionTypes,
    modelsActionTypes,
    shortCachesActionTypes,
    shortClustersActionTypes,
    shortIGFSsActionTypes,
    shortModelsActionTypes
} from '../reducer';

import {
    ADVANCED_SAVE_CACHE,
    ADVANCED_SAVE_CLUSTER,
    ADVANCED_SAVE_COMPLETE_CONFIGURATION,
    ADVANCED_SAVE_IGFS,
    ADVANCED_SAVE_MODEL,
    BASIC_SAVE,
    BASIC_SAVE_AND_DOWNLOAD,
    BASIC_SAVE_OK,
    COMPLETE_CONFIGURATION,
    CONFIRM_CLUSTERS_REMOVAL,
    CONFIRM_CLUSTERS_REMOVAL_OK,
    REMOVE_CLUSTER_ITEMS,
    REMOVE_CLUSTER_ITEMS_CONFIRMED
} from './actionTypes';

import {
    advancedSaveCompleteConfiguration,
    basicSaveErr,
    basicSaveOK,
    completeConfiguration,
    confirmClustersRemovalOK,
    removeClusterItemsConfirmed
} from './actionCreators';

import ConfigureState from 'app/components/page-configure/services/ConfigureState';
import ConfigurationDownload from 'app/components/page-configure/services/ConfigurationDownload';
import ConfigSelectors from 'app/components/page-configure/store/selectors';
import Clusters from 'app/services/Clusters';
import Caches from 'app/services/Caches';
import Models from 'app/services/Models';
import IGFSs from 'app/services/IGFSs';
import {Confirm} from 'app/services/Confirm.service';

export const ofType = (type) => (s) => s.filter((a) => a.type === type);

export default class ConfigEffects {
    static $inject = [
        'ConfigureState',
        'Caches',
        'IGFSs',
        'Models',
        'ConfigSelectors',
        'Clusters',
        '$state',
        'IgniteMessages',
        'IgniteConfirm',
        'Confirm',
        'ConfigurationDownload'
    ];

    /**
     * @param {ConfigureState} ConfigureState
     * @param {Caches} Caches
     * @param {IGFSs} IGFSs
     * @param {Models} Models
     * @param {ConfigSelectors} ConfigSelectors
     * @param {Clusters} Clusters
     * @param {object} $state
     * @param {object} IgniteMessages
     * @param {object} IgniteConfirm
     * @param {Confirm} Confirm
     * @param {ConfigurationDownload} ConfigurationDownload
     */
    constructor(ConfigureState, Caches, IGFSs, Models, ConfigSelectors, Clusters, $state, IgniteMessages, IgniteConfirm, Confirm, ConfigurationDownload) {
        this.ConfigureState = ConfigureState;
        this.ConfigSelectors = ConfigSelectors;
        this.IGFSs = IGFSs;
        this.Models = Models;
        this.Caches = Caches;
        this.Clusters = Clusters;
        this.$state = $state;
        this.IgniteMessages = IgniteMessages;
        this.IgniteConfirm = IgniteConfirm;
        this.Confirm = Confirm;
        this.configurationDownload = ConfigurationDownload;

        this.loadConfigurationEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_COMPLETE_CONFIGURATION'))
            .exhaustMap((action) => {
                return fromPromise(this.Clusters.getConfiguration(action.clusterID))
                    .switchMap(({data}) => of(
                        completeConfiguration(data),
                        {type: 'LOAD_COMPLETE_CONFIGURATION_OK', data}
                    ))
                    .catch((error) => of({
                        type: 'LOAD_COMPLETE_CONFIGURATION_ERR',
                        error: {
                            message: `Failed to load cluster configuration: ${error.data}.`
                        },
                        action
                    }));
            });

        this.storeConfigurationEffect$ = this.ConfigureState.actions$
            .let(ofType(COMPLETE_CONFIGURATION))
            .exhaustMap(({configuration: {cluster, caches, models, igfss}}) => of(...[
                cluster && {type: clustersActionTypes.UPSERT, items: [cluster]},
                caches && caches.length && {type: cachesActionTypes.UPSERT, items: caches},
                models && models.length && {type: modelsActionTypes.UPSERT, items: models},
                igfss && igfss.length && {type: igfssActionTypes.UPSERT, items: igfss}
            ].filter((v) => v)));

        this.saveCompleteConfigurationEffect$ = this.ConfigureState.actions$
            .let(ofType(ADVANCED_SAVE_COMPLETE_CONFIGURATION))
            .switchMap((action) => {
                const actions = [
                    {
                        type: modelsActionTypes.UPSERT,
                        items: action.changedItems.models
                    },
                    {
                        type: shortModelsActionTypes.UPSERT,
                        items: action.changedItems.models.map((m) => this.Models.toShortModel(m))
                    },
                    {
                        type: igfssActionTypes.UPSERT,
                        items: action.changedItems.igfss
                    },
                    {
                        type: shortIGFSsActionTypes.UPSERT,
                        items: action.changedItems.igfss
                    },
                    {
                        type: cachesActionTypes.UPSERT,
                        items: action.changedItems.caches
                    },
                    {
                        type: shortCachesActionTypes.UPSERT,
                        items: action.changedItems.caches.map(Caches.toShortCache)
                    },
                    {
                        type: clustersActionTypes.UPSERT,
                        items: [action.changedItems.cluster]
                    },
                    {
                        type: shortClustersActionTypes.UPSERT,
                        items: [Clusters.toShortCluster(action.changedItems.cluster)]
                    }
                ].filter((a) => a.items.length);

                return of(...actions)
                .merge(
                    fromPromise(Clusters.saveAdvanced(action.changedItems))
                    .switchMap((res) => {
                        return of(
                            {type: 'EDIT_CLUSTER', cluster: action.changedItems.cluster},
                            {type: 'ADVANCED_SAVE_COMPLETE_CONFIGURATION_OK', changedItems: action.changedItems}
                        );
                    })
                    .catch((res) => {
                        return of({
                            type: 'ADVANCED_SAVE_COMPLETE_CONFIGURATION_ERR',
                            changedItems: action.changedItems,
                            action,
                            error: {
                                message: `Failed to save cluster "${action.changedItems.cluster.name}": ${res.data}.`
                            }
                        }, {
                            type: 'UNDO_ACTIONS',
                            actions
                        });
                    })
                );
            });

        this.addCacheToEditEffect$ = this.ConfigureState.actions$
            .let(ofType('ADD_CACHE_TO_EDIT'))
            .switchMap(() => this.ConfigureState.state$.let(this.ConfigSelectors.selectCacheToEdit('new')).take(1))
            .map((cache) => ({type: 'UPSERT_CLUSTER_ITEM', itemType: 'caches', item: cache}));

        this.errorNotificationsEffect$ = this.ConfigureState.actions$
            .filter((a) => a.error)
            .do((action) => this.IgniteMessages.showError(action.error))
            .ignoreElements();

        this.loadUserClustersEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_USER_CLUSTERS'))
            .exhaustMap((a) => {
                return fromPromise(this.Clusters.getClustersOverview())
                    .switchMap(({data}) => of(
                        {type: shortClustersActionTypes.SET, items: data},
                        {type: `${a.type}_OK`}
                    ))
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load clusters: ${error.data}`
                        },
                        action: a
                    }));
            });

        this.loadAndEditClusterEffect$ = ConfigureState.actions$
            .let(ofType('LOAD_AND_EDIT_CLUSTER'))
            .withLatestFrom(this.ConfigureState.state$.let(this.ConfigSelectors.selectShortClustersValue()))
            .exhaustMap(([a, shortClusters]) => {
                if (a.clusterID === 'new') {
                    return of(
                        {
                            type: 'EDIT_CLUSTER',
                            cluster: {
                                ...this.Clusters.getBlankCluster(),
                                name: uniqueName(defaultNames.cluster, shortClusters)
                            }
                        },
                        {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
                    );
                }
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectCluster(a.clusterID)).take(1)
                    .switchMap((cluster) => {
                        if (cluster) {
                            return of(
                                {type: 'EDIT_CLUSTER', cluster},
                                {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
                            );
                        }
                        return from(this.Clusters.getCluster(a.clusterID))
                            .switchMap(({data}) => of(
                                {type: clustersActionTypes.UPSERT, items: [data]},
                                {type: 'EDIT_CLUSTER', cluster: data},
                                {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
                            ))
                            .catch((error) => of({
                                type: 'LOAD_AND_EDIT_CLUSTER_ERR',
                                error: {
                                    message: `Failed to load cluster: ${error.data}.`
                                }
                            }));
                    });
            });

        this.loadCacheEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_CACHE'))
            .exhaustMap((a) => {
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectCache(a.cacheID)).take(1)
                    .switchMap((cache) => {
                        if (cache)
                            return of({type: `${a.type}_OK`, cache});

                        return fromPromise(this.Caches.getCache(a.cacheID))
                            .switchMap(({data}) => of(
                                {type: 'CACHE', cache: data},
                                {type: `${a.type}_OK`, cache: data}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load cache: ${error.data}.`
                        }
                    }));
            });

        this.storeCacheEffect$ = this.ConfigureState.actions$
            .let(ofType('CACHE'))
            .map((a) => ({type: cachesActionTypes.UPSERT, items: [a.cache]}));

        this.loadShortCachesEffect$ = ConfigureState.actions$
            .let(ofType('LOAD_SHORT_CACHES'))
            .exhaustMap((a) => {
                if (!(a.ids || []).length)
                    return of({type: `${a.type}_OK`});

                return this.ConfigureState.state$.let(this.ConfigSelectors.selectShortCaches()).take(1)
                    .switchMap((items) => {
                        if (!items.pristine && a.ids && a.ids.every((_id) => items.value.has(_id)))
                            return of({type: `${a.type}_OK`});

                        return fromPromise(this.Clusters.getClusterCaches(a.clusterID))
                            .switchMap(({data}) => of(
                                {type: shortCachesActionTypes.UPSERT, items: data},
                                {type: `${a.type}_OK`}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load caches: ${error.data}.`
                        },
                        action: a
                    }));
            });

        this.loadIgfsEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_IGFS'))
            .exhaustMap((a) => {
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectIGFS(a.igfsID)).take(1)
                    .switchMap((igfs) => {
                        if (igfs)
                            return of({type: `${a.type}_OK`, igfs});

                        return fromPromise(this.IGFSs.getIGFS(a.igfsID))
                            .switchMap(({data}) => of(
                                {type: 'IGFS', igfs: data},
                                {type: `${a.type}_OK`, igfs: data}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load IGFS: ${error.data}.`
                        }
                    }));
            });

        this.storeIgfsEffect$ = this.ConfigureState.actions$
            .let(ofType('IGFS'))
            .map((a) => ({type: igfssActionTypes.UPSERT, items: [a.igfs]}));

        this.loadShortIgfssEffect$ = ConfigureState.actions$
            .let(ofType('LOAD_SHORT_IGFSS'))
            .exhaustMap((a) => {
                if (!(a.ids || []).length) {
                    return of(
                        {type: shortIGFSsActionTypes.UPSERT, items: []},
                        {type: `${a.type}_OK`}
                    );
                }
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectShortIGFSs()).take(1)
                    .switchMap((items) => {
                        if (!items.pristine && a.ids && a.ids.every((_id) => items.value.has(_id)))
                            return of({type: `${a.type}_OK`});

                        return fromPromise(this.Clusters.getClusterIGFSs(a.clusterID))
                            .switchMap(({data}) => of(
                                {type: shortIGFSsActionTypes.UPSERT, items: data},
                                {type: `${a.type}_OK`}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load IGFSs: ${error.data}.`
                        },
                        action: a
                    }));
            });

        this.loadModelEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_MODEL'))
            .exhaustMap((a) => {
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectModel(a.modelID)).take(1)
                    .switchMap((model) => {
                        if (model)
                            return of({type: `${a.type}_OK`, model});

                        return fromPromise(this.Models.getModel(a.modelID))
                            .switchMap(({data}) => of(
                                {type: 'MODEL', model: data},
                                {type: `${a.type}_OK`, model: data}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load domain model: ${error.data}.`
                        }
                    }));
            });

        this.storeModelEffect$ = this.ConfigureState.actions$
            .let(ofType('MODEL'))
            .map((a) => ({type: modelsActionTypes.UPSERT, items: [a.model]}));

        this.loadShortModelsEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_SHORT_MODELS'))
            .exhaustMap((a) => {
                if (!(a.ids || []).length) {
                    return of(
                        {type: shortModelsActionTypes.UPSERT, items: []},
                        {type: `${a.type}_OK`}
                    );
                }
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectShortModels()).take(1)
                    .switchMap((items) => {
                        if (!items.pristine && a.ids && a.ids.every((_id) => items.value.has(_id)))
                            return of({type: `${a.type}_OK`});

                        return fromPromise(this.Clusters.getClusterModels(a.clusterID))
                            .switchMap(({data}) => of(
                                {type: shortModelsActionTypes.UPSERT, items: data},
                                {type: `${a.type}_OK`}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load domain models: ${error.data}.`
                        },
                        action: a
                    }));
            });

        this.basicSaveRedirectEffect$ = this.ConfigureState.actions$
            .let(ofType(BASIC_SAVE_OK))
            .do((a) => this.$state.go('base.configuration.edit.basic', {clusterID: a.changedItems.cluster._id}, {location: 'replace', custom: {justIDUpdate: true}}))
            .ignoreElements();

        this.basicDownloadAfterSaveEffect$ = this.ConfigureState.actions$.let(ofType(BASIC_SAVE_AND_DOWNLOAD))
            .zip(this.ConfigureState.actions$.let(ofType(BASIC_SAVE_OK)))
            .pluck('1')
            .do((a) => this.configurationDownload.downloadClusterConfiguration(a.changedItems.cluster))
            .ignoreElements();

        this.advancedDownloadAfterSaveEffect$ = merge(
            this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_CLUSTER)),
            this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_CACHE)),
            this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_IGFS)),
            this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_MODEL)),
        )
            .filter((a) => a.download)
            .zip(this.ConfigureState.actions$.let(ofType('ADVANCED_SAVE_COMPLETE_CONFIGURATION_OK')))
            .pluck('1')
            .do((a) => this.configurationDownload.downloadClusterConfiguration(a.changedItems.cluster))
            .ignoreElements();

        this.advancedSaveRedirectEffect$ = this.ConfigureState.actions$
            .let(ofType('ADVANCED_SAVE_COMPLETE_CONFIGURATION_OK'))
            .withLatestFrom(this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_COMPLETE_CONFIGURATION)))
            .pluck('1', 'changedItems')
            .map((req) => {
                const firstChangedItem = Object.keys(req).filter((k) => k !== 'cluster')
                    .map((k) => Array.isArray(req[k]) ? [k, req[k][0]] : [k, req[k]])
                    .filter((v) => v[1])
                    .pop();
                return firstChangedItem ? [...firstChangedItem, req.cluster] : ['cluster', req.cluster, req.cluster];
            })
            .do(([type, value, cluster]) => {
                const go = (state, params = {}) => this.$state.go(
                    state, {...params, clusterID: cluster._id}, {location: 'replace', custom: {justIDUpdate: true}}
                );

                switch (type) {
                    case 'models': {
                        const state = 'base.configuration.edit.advanced.models.model';
                        this.IgniteMessages.showInfo(`Model "${value.valueType}" saved`);

                        if (this.$state.is(state) && this.$state.params.modelID !== value._id)
                            return go(state, {modelID: value._id});

                        break;
                    }

                    case 'caches': {
                        const state = 'base.configuration.edit.advanced.caches.cache';
                        this.IgniteMessages.showInfo(`Cache "${value.name}" saved`);

                        if (this.$state.is(state) && this.$state.params.cacheID !== value._id)
                            return go(state, {cacheID: value._id});

                        break;
                    }

                    case 'igfss': {
                        const state = 'base.configuration.edit.advanced.igfs.igfs';
                        this.IgniteMessages.showInfo(`IGFS "${value.name}" saved`);

                        if (this.$state.is(state) && this.$state.params.igfsID !== value._id)
                            return go(state, {igfsID: value._id});

                        break;
                    }

                    case 'cluster': {
                        const state = 'base.configuration.edit.advanced.cluster';
                        this.IgniteMessages.showInfo(`Cluster "${value.name}" saved`);

                        if (this.$state.is(state) && this.$state.params.clusterID !== value._id)
                            return go(state);

                        break;
                    }

                    default: break;
                }
            })
            .ignoreElements();

        this.removeClusterItemsEffect$ = this.ConfigureState.actions$
            .let(ofType(REMOVE_CLUSTER_ITEMS))
            .exhaustMap((a) => {
                return a.confirm
                    // TODO: list items to remove in confirmation
                    ? fromPromise(this.Confirm.confirm('Are you sure you want to remove these items?'))
                        .mapTo(a)
                        .catch(() => empty())
                    : of(a);
            })
            .map((a) => removeClusterItemsConfirmed(a.clusterID, a.itemType, a.itemIDs));

        this.persistRemovedClusterItemsEffect$ = this.ConfigureState.actions$
            .let(ofType(REMOVE_CLUSTER_ITEMS_CONFIRMED))
            .withLatestFrom(this.ConfigureState.actions$.let(ofType(REMOVE_CLUSTER_ITEMS)))
            .filter(([a, b]) => {
                return a.itemType === b.itemType
                    && b.save
                    && JSON.stringify(a.itemIDs) === JSON.stringify(b.itemIDs);
            })
            .pluck('0')
            .withLatestFrom(this.ConfigureState.state$.pluck('edit'))
            .map(([action, edit]) => advancedSaveCompleteConfiguration(edit));

        this.confirmClustersRemovalEffect$ = this.ConfigureState.actions$
            .let(ofType(CONFIRM_CLUSTERS_REMOVAL))
            .pluck('clusterIDs')
            .switchMap((ids) => this.ConfigureState.state$.let(this.ConfigSelectors.selectClusterNames(ids)).take(1))
            .exhaustMap((names) => {
                return fromPromise(this.Confirm.confirm(`
                    <p>Are you sure you want to remove these clusters?</p>
                    <ul>${names.map((name) => `<li>${name}</li>`).join('')}</ul>
                `))
                .map(confirmClustersRemovalOK)
                .catch(() => Observable.empty());
            });

        this.persistRemovedClustersLocallyEffect$ = this.ConfigureState.actions$
            .let(ofType(CONFIRM_CLUSTERS_REMOVAL_OK))
            .withLatestFrom(this.ConfigureState.actions$.let(ofType(CONFIRM_CLUSTERS_REMOVAL)))
            .switchMap(([, {clusterIDs}]) => of(
                {type: shortClustersActionTypes.REMOVE, ids: clusterIDs},
                {type: clustersActionTypes.REMOVE, ids: clusterIDs}
            ));

        this.persistRemovedClustersRemotelyEffect$ = this.ConfigureState.actions$
            .let(ofType(CONFIRM_CLUSTERS_REMOVAL_OK))
            .withLatestFrom(
                this.ConfigureState.actions$.let(ofType(CONFIRM_CLUSTERS_REMOVAL)),
                this.ConfigureState.actions$.let(ofType(shortClustersActionTypes.REMOVE)),
                this.ConfigureState.actions$.let(ofType(clustersActionTypes.REMOVE))
            )
            .switchMap(([, {clusterIDs}, ...backup]) => this.Clusters.removeCluster$(clusterIDs)
                .mapTo({
                    type: 'REMOVE_CLUSTERS_OK'
                })
                .catch((e) => of(
                    {
                        type: 'REMOVE_CLUSTERS_ERR',
                        error: {
                            message: `Failed to remove clusters: ${e.data}`
                        }
                    },
                    {
                        type: 'UNDO_ACTIONS',
                        actions: backup
                    }
                ))
            );

        this.notifyRemoteClustersRemoveSuccessEffect$ = this.ConfigureState.actions$
            .let(ofType('REMOVE_CLUSTERS_OK'))
            .withLatestFrom(this.ConfigureState.actions$.let(ofType(CONFIRM_CLUSTERS_REMOVAL)))
            .do(([, {clusterIDs}]) => this.IgniteMessages.showInfo(`Cluster(s) removed: ${clusterIDs.length}`))
            .ignoreElements();

        const _applyChangedIDs = (edit, {cache, igfs, model, cluster} = {}) => ({
            cluster: {
                ...edit.changes.cluster,
                ...(cluster ? cluster : {}),
                caches: cache ? uniq([...edit.changes.caches.ids, cache._id]) : edit.changes.caches.ids,
                igfss: igfs ? uniq([...edit.changes.igfss.ids, igfs._id]) : edit.changes.igfss.ids,
                models: model ? uniq([...edit.changes.models.ids, model._id]) : edit.changes.models.ids
            },
            caches: cache ? uniq([...edit.changes.caches.changedItems, cache]) : edit.changes.caches.changedItems,
            igfss: igfs ? uniq([...edit.changes.igfss.changedItems, igfs]) : edit.changes.igfss.changedItems,
            models: model ? uniq([...edit.changes.models.changedItems, model]) : edit.changes.models.changedItems
        });

        this.advancedSaveCacheEffect$ = merge(
            this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_CLUSTER)),
            this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_CACHE)),
            this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_IGFS)),
            this.ConfigureState.actions$.let(ofType(ADVANCED_SAVE_MODEL)),
        )
            .withLatestFrom(this.ConfigureState.state$.pluck('edit'))
            .map(([action, edit]) => ({
                type: ADVANCED_SAVE_COMPLETE_CONFIGURATION,
                changedItems: _applyChangedIDs(edit, action)
            }));

        this.basicSaveEffect$ = this.ConfigureState.actions$
            .let(ofType(BASIC_SAVE))
            .merge(this.ConfigureState.actions$.let(ofType(BASIC_SAVE_AND_DOWNLOAD)))
            .withLatestFrom(this.ConfigureState.state$.pluck('edit'))
            .switchMap(([action, edit]) => {
                const changedItems = _applyChangedIDs(edit, {cluster: action.cluster});
                const actions = [{
                    type: cachesActionTypes.UPSERT,
                    items: changedItems.caches
                },
                {
                    type: shortCachesActionTypes.UPSERT,
                    items: changedItems.caches
                },
                {
                    type: clustersActionTypes.UPSERT,
                    items: [changedItems.cluster]
                },
                {
                    type: shortClustersActionTypes.UPSERT,
                    items: [this.Clusters.toShortCluster(changedItems.cluster)]
                }
                ].filter((a) => a.items.length);

                return Observable.of(...actions)
                .merge(
                    Observable.fromPromise(this.Clusters.saveBasic(changedItems))
                    .switchMap((res) => Observable.of(
                        {type: 'EDIT_CLUSTER', cluster: changedItems.cluster},
                        basicSaveOK(changedItems)
                    ))
                    .catch((res) => Observable.of(
                        basicSaveErr(changedItems, res),
                        {type: 'UNDO_ACTIONS', actions}
                    ))
                );
            });

        this.basicSaveOKMessagesEffect$ = this.ConfigureState.actions$
            .let(ofType(BASIC_SAVE_OK))
            .do((action) => this.IgniteMessages.showInfo(`Cluster "${action.changedItems.cluster.name}" saved.`))
            .ignoreElements();
    }

    /**
     * @name etp
     * @function
     * @param {object} action
     * @returns {Promise}
     */
    /**
     * @name etp^2
     * @function
     * @param {string} type
     * @param {object} [params]
     * @returns {Promise}
     */
    etp = (...args) => {
        const action = typeof args[0] === 'object' ? args[0] : {type: args[0], ...args[1]};
        const ok = `${action.type}_OK`;
        const err = `${action.type}_ERR`;

        setTimeout(() => this.ConfigureState.dispatchAction(action));

        return this.ConfigureState.actions$
            .filter((a) => a.type === ok || a.type === err)
            .take(1)
            .map((a) => {
                if (a.type === err)
                    throw a;
                else
                    return a;
            })
            .toPromise();
    };

    connect() {
        return merge(
            ...Object.keys(this).filter((k) => k.endsWith('Effect$')).map((k) => this[k])
        ).do((a) => this.ConfigureState.dispatchAction(a)).subscribe();
    }
}
