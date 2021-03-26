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

package org.apache.ignite.console.services;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.springframework.stereotype.Service;

/**
 * Service to handle configurations.
 */
@Service
public class ConfigurationsService {
    /** Repository to work with configurations. */
    private final ConfigurationsRepository cfgsRepo;

    /**
     * @param cfgsRepo Configurations repository.
     */
    public ConfigurationsService(ConfigurationsRepository cfgsRepo) {
        this.cfgsRepo = cfgsRepo;
    }

    /**
     * Delete all notebook for specified account.
     *
     * @param accId Account ID.
     */
    void deleteByAccountId(UUID accId) {
        cfgsRepo.deleteByAccountId(new ConfigurationKey(accId, true));
        cfgsRepo.deleteByAccountId(new ConfigurationKey(accId, false));
    }

    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Configuration.
     */
    public JsonObject loadConfiguration(ConfigurationKey key, UUID clusterId) {
        return cfgsRepo.loadConfiguration(key, clusterId);
    }

    /**
     * @param key Account ID.
     * @return List of clusters for specified account.
     */
    public JsonArray loadClusters(ConfigurationKey key) {
        return cfgsRepo.loadClusters(key);
    }

    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Cluster.
     */
    public String loadCluster(ConfigurationKey key, UUID clusterId) {
        return cfgsRepo.loadCluster(key, clusterId).json();
    }

    /**
     * @param key Configuration key.
     * @param cacheId Cache ID.
     * @return Cache.
     */
    public String loadCache(ConfigurationKey key, UUID cacheId) {
        return cfgsRepo.loadCache(key, cacheId).json();
    }

    /**
     * @param key Configuration key.
     * @param mdlId Model ID.
     * @return Model.
     */
    public String loadModel(ConfigurationKey key, UUID mdlId) {
        return cfgsRepo.loadModel(key, mdlId).json();
    }

    /**
     * Convert list of DTOs to short view.
     *
     * @param items List of DTOs to convert.
     * @return List of short objects.
     */
    private JsonArray toShortList(Collection<? extends DataObject> items) {
        JsonArray res = new JsonArray();

        items.forEach(item -> res.add(item.shortView()));

        return res;
    }

    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Collection of cluster caches.
     */
    public JsonArray loadShortCaches(ConfigurationKey key, UUID clusterId) {
        return toShortList(cfgsRepo.loadCaches(key, clusterId));
    }

    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Collection of cluster models.
     */
    public JsonArray loadShortModels(ConfigurationKey key, UUID clusterId) {
        return toShortList(cfgsRepo.loadModels(key, clusterId));
    }

    /**
     * Save full cluster.
     *
     * @param key Configuration key.
     * @param changedItems Items to save.
     */
    public void saveAdvancedCluster(ConfigurationKey key, JsonObject changedItems) {
        cfgsRepo.saveAdvancedCluster(key, changedItems);
    }

    /**
     * Save basic cluster.
     *
     * @param key Configuration key.
     * @param changedItems Items to save.
     */
    public void saveBasicCluster(ConfigurationKey key, JsonObject changedItems) {
        cfgsRepo.saveBasicCluster(key, changedItems);
    }

    /**
     * Delete clusters.
     *
     * @param key Configuration key.
     * @param clusterIds Clusters IDs to delete.
     */
    public void deleteClusters(ConfigurationKey key, TreeSet<UUID> clusterIds) {
        cfgsRepo.deleteClusters(key, clusterIds);
    }
}
