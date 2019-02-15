/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import {
    REMOVE_CLUSTER_ITEMS,
    REMOVE_CLUSTER_ITEMS_CONFIRMED,
    ADVANCED_SAVE_COMPLETE_CONFIGURATION,
    CONFIRM_CLUSTERS_REMOVAL,
    CONFIRM_CLUSTERS_REMOVAL_OK,
    COMPLETE_CONFIGURATION,
    ADVANCED_SAVE_CLUSTER,
    ADVANCED_SAVE_CACHE,
    ADVANCED_SAVE_IGFS,
    ADVANCED_SAVE_MODEL,
    BASIC_SAVE,
    BASIC_SAVE_AND_DOWNLOAD,
    BASIC_SAVE_OK,
    BASIC_SAVE_ERR
} from './actionTypes';

/**
 * @typedef {object} IRemoveClusterItemsAction
 * @prop {'REMOVE_CLUSTER_ITEMS'} type
 * @prop {('caches'|'igfss'|'models')} itemType
 * @prop {string} clusterID
 * @prop {Array<string>} itemIDs
 * @prop {boolean} save
 * @prop {boolean} confirm
 */

/**
 * @param {string} clusterID
 * @param {('caches'|'igfss'|'models')} itemType
 * @param {Array<string>} itemIDs
 * @param {boolean} [save=false]
 * @param {boolean} [confirm=true]
 * @returns {IRemoveClusterItemsAction}
 */
export const removeClusterItems = (clusterID, itemType, itemIDs, save = false, confirm = true) => ({
    type: REMOVE_CLUSTER_ITEMS,
    itemType,
    clusterID,
    itemIDs,
    save,
    confirm
});

/**
 * @typedef {object} IRemoveClusterItemsConfirmed
 * @prop {string} clusterID
 * @prop {'REMOVE_CLUSTER_ITEMS_CONFIRMED'} type
 * @prop {('caches'|'igfss'|'models')} itemType
 * @prop {Array<string>} itemIDs
 */

/**
 * @param {string} clusterID
 * @param {(('caches'|'igfss'|'models'))} itemType
 * @param {Array<string>} itemIDs
 * @returns {IRemoveClusterItemsConfirmed}
 */
export const removeClusterItemsConfirmed = (clusterID, itemType, itemIDs) => ({
    type: REMOVE_CLUSTER_ITEMS_CONFIRMED,
    itemType,
    clusterID,
    itemIDs
});

const applyChangedIDs = (edit) => ({
    cluster: {
        ...edit.changes.cluster,
        caches: edit.changes.caches.ids,
        igfss: edit.changes.igfss.ids,
        models: edit.changes.models.ids
    },
    caches: edit.changes.caches.changedItems,
    igfss: edit.changes.igfss.changedItems,
    models: edit.changes.models.changedItems
});

const upsertCluster = (cluster) => ({
    type: 'UPSERT_CLUSTER',
    cluster
});

export const changeItem = (type, item) => ({
    type: 'UPSERT_CLUSTER_ITEM',
    itemType: type,
    item
});

/**
 * @typedef {object} IAdvancedSaveCompleteConfigurationAction
 * @prop {'ADVANCED_SAVE_COMPLETE_CONFIGURATION'} type
 * @prop {object} changedItems
 * @prop {Array<object>} [prevActions]
 */

/**
 * @returns {IAdvancedSaveCompleteConfigurationAction}
 */
// TODO: add support for prev actions
export const advancedSaveCompleteConfiguration = (edit) => {
    return {
        type: ADVANCED_SAVE_COMPLETE_CONFIGURATION,
        changedItems: applyChangedIDs(edit)
    };
};

/**
 * @typedef {object} IConfirmClustersRemovalAction
 * @prop {'CONFIRM_CLUSTERS_REMOVAL'} type
 * @prop {Array<string>} clusterIDs
 */

/**
 * @param {Array<string>} clusterIDs
 * @returns {IConfirmClustersRemovalAction}
 */
export const confirmClustersRemoval = (clusterIDs) => ({
    type: CONFIRM_CLUSTERS_REMOVAL,
    clusterIDs
});

/**
 * @typedef {object} IConfirmClustersRemovalActionOK
 * @prop {'CONFIRM_CLUSTERS_REMOVAL_OK'} type
 */

/**
 * @returns {IConfirmClustersRemovalActionOK}
 */
export const confirmClustersRemovalOK = () => ({
    type: CONFIRM_CLUSTERS_REMOVAL_OK
});

export const completeConfiguration = (configuration) => ({
    type: COMPLETE_CONFIGURATION,
    configuration
});

export const advancedSaveCluster = (cluster, download = false) => ({type: ADVANCED_SAVE_CLUSTER, cluster, download});
export const advancedSaveCache = (cache, download = false) => ({type: ADVANCED_SAVE_CACHE, cache, download});
export const advancedSaveIGFS = (igfs, download = false) => ({type: ADVANCED_SAVE_IGFS, igfs, download});
export const advancedSaveModel = (model, download = false) => ({type: ADVANCED_SAVE_MODEL, model, download});

export const basicSave = (cluster) => ({type: BASIC_SAVE, cluster});
export const basicSaveAndDownload = (cluster) => ({type: BASIC_SAVE_AND_DOWNLOAD, cluster});
export const basicSaveOK = (changedItems) => ({type: BASIC_SAVE_OK, changedItems});
export const basicSaveErr = (changedItems, res) => ({
    type: BASIC_SAVE_ERR,
    changedItems,
    error: {
        message: `Failed to save cluster "${changedItems.cluster.name}": ${res.data}.`
    }
});
