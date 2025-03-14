

import {
    REMOVE_CLUSTER_ITEMS,
    REMOVE_CLUSTER_ITEMS_CONFIRMED,
    ADVANCED_SAVE_COMPLETE_CONFIGURATION,
    CONFIRM_CLUSTERS_REMOVAL,
    CONFIRM_CLUSTERS_REMOVAL_OK,
    COMPLETE_CONFIGURATION,
    ADVANCED_SAVE_CLUSTER,
    ADVANCED_SAVE_CACHE,
    ADVANCED_SAVE_MODEL,
    BASIC_SAVE,
    BASIC_SAVE_AND_DOWNLOAD,
    BASIC_SAVE_OK,
    BASIC_SAVE_ERR
} from './actionTypes';

/**
 * @typedef {object} IRemoveClusterItemsAction
 * @prop {'REMOVE_CLUSTER_ITEMS'} type
 * @prop {('caches'|'models')} itemType
 * @prop {string} clusterID
 * @prop {Array<string>} itemIDs
 * @prop {boolean} save
 * @prop {boolean} confirm
 */

/**
 * @param {string} clusterID
 * @param {('caches'|'models')} itemType
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
 * @prop {('caches'|'models')} itemType
 * @prop {Array<string>} itemIDs
 */

/**
 * @param {string} clusterID
 * @param {(('caches'|'models'))} itemType
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
        models: edit.changes.models.ids
    },
    caches: edit.changes.caches.changedItems,
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
