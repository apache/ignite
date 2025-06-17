

package org.apache.ignite.console.services;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.ignite.console.dto.*;

import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.springframework.stereotype.Service;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

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
    public Cluster loadCluster(ConfigurationKey key, UUID clusterId) {
        return cfgsRepo.loadCluster(key, clusterId);
    }

    /**
     * @param key Configuration key.
     * @param cacheId Cache ID.
     * @return Cache.
     */
    public Cache loadCache(ConfigurationKey key, UUID cacheId) {
        return cfgsRepo.loadCache(key, cacheId);
    }

    /**
     * @param key Configuration key.
     * @param tableName String.
     * @return Model.
     */
    public Model loadModel(ConfigurationKey key, String catalog, String schema,String tableName) {
        return cfgsRepo.loadModel(key, catalog, schema, tableName);
    }
    
    /**
     * @param key Configuration key.
     * @param mdlId Model ID.
     * @return Model.
     */
    public Model loadModel(ConfigurationKey key, UUID mdlId) {
        return cfgsRepo.loadModel(key, mdlId);
    }

    public IGFS loadIGFS(ConfigurationKey key, UUID mdlId) {
        return cfgsRepo.loadIGFS(key, mdlId);
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

    public JsonArray loadShortIGFSs(ConfigurationKey key, UUID clusterId) {
        return toShortList(cfgsRepo.loadIGFSs(key, clusterId));
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
