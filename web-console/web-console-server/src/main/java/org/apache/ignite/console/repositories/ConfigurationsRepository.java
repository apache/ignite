

package org.apache.ignite.console.repositories;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.*;

import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.messages.WebConsoleMessageSourceAccessor;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.stereotype.Repository;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.console.common.Utils.diff;
import static org.apache.ignite.console.common.Utils.idsFromJson;
import static org.apache.ignite.console.common.Utils.toJsonArray;
import static org.apache.ignite.console.utils.Utils.asJson;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * Repository to work with configurations.
 */
@Repository
public class ConfigurationsRepository {
    /** */
    protected final TransactionManager txMgr;

    /** Messages accessor. */
    private final WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** */
    private Table<Cluster> clustersTbl;

    /** */
    private Table<Cache> cachesTbl;

    /** */
    private Table<Model> modelsTbl;

    private Table<IGFS> igfssTbl;

    /** */
    private OneToManyIndex<UUID, UUID> cachesIdx;

    /** */
    private OneToManyIndex<UUID, UUID> modelsIdx;

    /** */
    private OneToManyIndex<UUID, UUID> igfssIdx;

    /** */
    protected OneToManyIndex<ConfigurationKey, UUID> clustersIdx;

    /** */
    private OneToManyIndex<ConfigurationKey, UUID> cfgIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public ConfigurationsRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            clustersTbl = new Table<>(ignite, "wc_account_clusters");
            cachesTbl = new Table<>(ignite, "wc_cluster_caches");
            modelsTbl = new Table<>(ignite, "wc_cluster_models");
            igfssTbl = new Table<>(ignite, "wc_cluster_igfss");

            cachesIdx = new OneToManyIndex<>(ignite, "wc_cluster_caches_idx");
            modelsIdx = new OneToManyIndex<>(ignite, "wc_cluster_models_idx");
            igfssIdx = new OneToManyIndex<>(ignite, "wc_cluster_igfss_idx");
            clustersIdx = new OneToManyIndex<>(ignite, "wc_account_clusters_idx");
            cfgIdx = new OneToManyIndex<>(ignite, "wc_account_configs_idx");
        });
    }

    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Configuration in JSON format.
     */
    public JsonObject loadConfiguration(ConfigurationKey key, UUID clusterId) {
        return txMgr.doInTransaction(() -> {
            Cluster cluster = clustersTbl.get(clusterId);

            if (cluster == null)
                throw new IllegalStateException(messages.getMessageWithArgs("err.cluster-not-found-by-id", clusterId));

            clustersIdx.validate(key, clusterId);

            Collection<Cache> caches = cachesTbl.loadAll(cachesIdx.get(clusterId));
            Collection<Model> models = modelsTbl.loadAll(modelsIdx.get(clusterId));
            Collection<IGFS> igfss = igfssTbl.loadAll(igfssIdx.get(clusterId));

            return new JsonObject().put("demo", key.isDemo())
                .put("cluster", fromJson(cluster.json()))
                .put("caches", toJsonArray(caches))
                .put("igfss", toJsonArray(igfss))
                .put("models", toJsonArray(models));
        });
    }

    /**
     * @param cluster Cluster DTO.
     * @return Short view of cluster DTO as JSON object.
     */
    protected JsonObject shortCluster(Cluster cluster) {
        UUID clusterId = cluster.getId();

        int cachesCnt = cachesIdx.get(clusterId).size();
        int modelsCnt = modelsIdx.get(clusterId).size();
        int igfssCnt = igfssIdx.get(clusterId).size();
        return new JsonObject()
            .put("id", cluster.getId())
            .put("name", cluster.name())
            .put("comment", cluster.comment())
            .put("discovery", cluster.discovery())
            .put("cachesCount", cachesCnt)
            .put("igfssCount", igfssCnt)
            .put("modelsCount", modelsCnt);
    }

    /**
     * @param key Configuration key.
     * @return List of user clusters.
     */
    public JsonArray loadClusters(ConfigurationKey key) {
        return txMgr.doInTransaction(() -> {
            Set<UUID> clusterIds = clustersIdx.get(key);

            Collection<Cluster> clusters = clustersTbl.loadAll(clusterIds);

            JsonArray shortList = new JsonArray();

            clusters.forEach(cluster -> shortList.add(shortCluster(cluster)));

            return shortList;
        });
    }

    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Cluster.
     */
    public Cluster loadCluster(ConfigurationKey key, UUID clusterId) {
        return txMgr.doInTransaction(() -> {
            Cluster cluster = clustersTbl.get(clusterId);

            if (cluster == null)
                throw new IllegalStateException(messages.getMessageWithArgs("err.cluster-not-found-by-id", clusterId));

            clustersIdx.validate(key, clusterId);

            return cluster;
        });
    }

    /**
     * @param key Configuration key.
     * @param cacheId Cache ID.
     * @return Cache.
     */
    public Cache loadCache(ConfigurationKey key, UUID cacheId) {
        return txMgr.doInTransaction(() -> {
            Cache cache = cachesTbl.get(cacheId);

            if (cache == null)
                throw new IllegalStateException(messages.getMessageWithArgs("err.cache-not-found-by-id", cacheId));

            cfgIdx.validate(key, cacheId);

            return cache;
        });
    }

    /**
     * @param key Configuration key.
     * @param mdlId Model ID.
     * @return Model.
     */
    public Model loadModel(ConfigurationKey key, UUID mdlId) {
        return txMgr.doInTransaction(() -> {
            Model mdl = modelsTbl.get(mdlId);

            if (mdl == null)
                throw new IllegalStateException(messages.getMessageWithArgs("err.model-not-found-by-id", mdlId));

            cfgIdx.validate(key, mdlId);

            return mdl;
        });
    }

    public IGFS loadIGFS(ConfigurationKey key, UUID mdlId) {
        return txMgr.doInTransaction(() -> {
            IGFS mdl = igfssTbl.get(mdlId);

            if (mdl == null)
                throw new IllegalStateException(messages.getMessageWithArgs("err.igfs-not-found-by-id", mdlId));

            cfgIdx.validate(key, mdlId);

            return mdl;
        });
    }

    /**
     * @param key Configuration key.
     * @param catalog/schema/tableName Model Path.
     * @return Model.
     */
    @SuppressWarnings("unused")
	public Model loadModel(ConfigurationKey key, String catalog, String schema, String tableName) {
        return txMgr.doInTransaction(() -> {
        	Set<UUID> clusterIds = clustersIdx.get(key);
        	String clusterName = catalog+"_"+schema;        	

            Collection<Cluster> clusters = clustersTbl.loadAll(clusterIds);
            Model theMdl = null;
            UUID clusterId = null;
            for(Cluster cluster: clusters) {
            	if(cluster.name().equalsIgnoreCase(catalog) || cluster.name().equalsIgnoreCase(clusterName)) {
            		clusterId = cluster.getId();            		
            		break;
            	}            	
            }
            if(clusterId==null) {
            	if(catalog.equalsIgnoreCase("ignite")) {
            		clusterName = "hive_"+ schema;
            	}
            	for(Cluster cluster: clusters) {
                	if(cluster.name().equalsIgnoreCase(schema) || cluster.name().equalsIgnoreCase(clusterName)) {
                		clusterId = cluster.getId();            		
                		break;
                	}            	
                }
            }
            if (clusterId == null)
                throw new IllegalStateException(messages.getMessageWithArgs("err.model-not-found-by-name", catalog));

            Collection<Model> models = modelsTbl.loadAll(modelsIdx.get(clusterId));
    		for(Model mdl: models) {
    			if(mdl.valueType().equalsIgnoreCase(tableName)) {
    				theMdl = mdl;
    				return mdl;
    			}
    		}
    		
            if (theMdl == null)
                throw new IllegalStateException(messages.getMessageWithArgs("err.model-not-found-by-name", tableName));
            return theMdl;

        });
    }
    
    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Collection of cluster caches.
     */
    public Collection<Cache> loadCaches(ConfigurationKey key, UUID clusterId) {
        return txMgr.doInTransaction(() -> {
            clustersIdx.validate(key, clusterId);

            Set<UUID> cachesIds = cachesIdx.get(clusterId);

            cfgIdx.validateAll(key, cachesIds);

            return cachesTbl.loadAll(cachesIds);
        });
    }

    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Collection of cluster models.
     */
    public Collection<Model> loadModels(ConfigurationKey key, UUID clusterId) {
        return txMgr.doInTransaction(() -> {
            clustersIdx.validate(key, clusterId);

            Set<UUID> modelsIds = modelsIdx.get(clusterId);

            cfgIdx.validateAll(key, modelsIds);

            return modelsTbl.loadAll(modelsIds);
        });
    }

    /**
     * @param key Configuration key.
     * @param clusterId Cluster ID.
     * @return Collection of cluster models.
     */
    public Collection<IGFS> loadIGFSs(ConfigurationKey key, UUID clusterId) {
        return txMgr.doInTransaction(() -> {
            clustersIdx.validate(key, clusterId);

            Set<UUID> modelsIds = igfssIdx.get(clusterId);

            cfgIdx.validateAll(key, modelsIds);

            return igfssTbl.loadAll(modelsIds);
        });
    }

    /**
     * Handle objects that was deleted from cluster.
     *
     * @param key Configuration key.
     * @param tbl Table with DTOs.
     * @param idx Foreign key.
     * @param clusterId Cluster ID.
     * @param oldCluster Old cluster JSON.
     * @param newCluster New cluster JSON.
     * @param fld Field name that holds IDs to check for deletion.
     */
    private void removedInCluster(
        ConfigurationKey key,
        Table<? extends DataObject> tbl,
        OneToManyIndex<UUID, UUID> idx,
        UUID clusterId,
        JsonObject oldCluster,
        JsonObject newCluster,
        String fld
    ) {
        TreeSet<UUID> oldIds = idsFromJson(oldCluster, fld);
        TreeSet<UUID> newIds = idsFromJson(newCluster, fld);

        TreeSet<UUID> deletedIds = diff(oldIds, newIds);

        if (!F.isEmpty(deletedIds)) {
            cfgIdx.validateAll(key, deletedIds);

            tbl.deleteAll(deletedIds);
            idx.removeAll(clusterId, deletedIds);
        }
    }

    /**
     * @param key Configuration key.
     * @param changedItems Items to save.
     * @return Saved cluster.
     */
    private Cluster saveCluster(ConfigurationKey key, JsonObject changedItems) {
        JsonObject jsonCluster = changedItems.getJsonObject("cluster");

        Cluster newCluster = Cluster.fromJson(jsonCluster);

        UUID clusterId = newCluster.getId();

        clustersIdx.validateBeforeSave(key, clusterId, clustersTbl);

        Cluster oldCluster = clustersTbl.get(clusterId);

        if (oldCluster != null) {
            JsonObject oldClusterJson = fromJson(oldCluster.json());

            removedInCluster(key, cachesTbl, cachesIdx, clusterId, oldClusterJson, jsonCluster, "caches");
            removedInCluster(key, modelsTbl, modelsIdx, clusterId, oldClusterJson, jsonCluster, "models");
        }

        clustersTbl.save(newCluster);

        clustersIdx.add(key, clusterId);

        return newCluster;
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     * @param basic {@code true} in case of saving basic cluster.
     */
    private void saveCaches(ConfigurationKey key, Cluster cluster, JsonObject json, boolean basic) {
        JsonArray jsonCaches = json.getJsonArray("caches");

        if (F.isEmpty(jsonCaches))
            return;

        Map<UUID, Cache> caches = jsonCaches
            .stream()
            .map(item -> Cache.fromJson(asJson(item)))
            .collect(toMap(Cache::getId, c -> c));

        Set<UUID> cacheIds = caches.keySet();

        cfgIdx.validateBeforeSave(key, cacheIds, cachesTbl);

        if (basic) {
            Collection<Cache> oldCaches = cachesTbl.loadAll(new TreeSet<>(cacheIds));

            oldCaches.forEach(oldCache -> {
                Cache newCache = caches.get(oldCache.getId());

                if (newCache != null) {
                    JsonObject oldJson = fromJson(oldCache.json());
                    JsonObject newJson = fromJson(newCache.json());

                    newCache.json(toJson(oldJson.mergeIn(newJson)));
                }
            });
        }

        cfgIdx.addAll(key, cacheIds);

        cachesIdx.addAll(cluster.getId(), cacheIds);

        cachesTbl.saveAll(caches);
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     */
    private void saveModels(ConfigurationKey key, Cluster cluster, JsonObject json) {
        JsonArray jsonModels = json.getJsonArray("models");

        if (F.isEmpty(jsonModels))
            return;

        Map<UUID, Model> mdls = jsonModels
            .stream()
            .map(item -> Model.fromJson(asJson(item)))
            .collect(toMap(Model::getId, m -> m));

        Set<UUID> mdlIds = mdls.keySet();

        cfgIdx.validateBeforeSave(key, mdlIds, modelsTbl);

        cfgIdx.addAll(key, mdlIds);

        modelsIdx.addAll(cluster.getId(), mdlIds);

        modelsTbl.saveAll(mdls);
    }

    private void saveIGFSs(ConfigurationKey key, Cluster cluster, JsonObject json) {
        JsonArray jsonModels = json.getJsonArray("igfss");

        if (F.isEmpty(jsonModels))
            return;

        Map<UUID, IGFS> mdls = jsonModels
                .stream()
                .map(item -> IGFS.fromJson(asJson(item)))
                .collect(toMap(IGFS::getId, m -> m));

        Set<UUID> mdlIds = mdls.keySet();

        cfgIdx.validateBeforeSave(key, mdlIds, modelsTbl);

        cfgIdx.addAll(key, mdlIds);

        igfssIdx.addAll(cluster.getId(), mdlIds);

        igfssTbl.saveAll(mdls);
    }

    /**
     * Save full cluster.
     *
     * @param key Configuration key.
     * @param json Configuration in JSON format.
     */
    public void saveAdvancedCluster(ConfigurationKey key, JsonObject json) {
        txMgr.doInTransaction(() -> {
            Cluster cluster = saveCluster(key, json);

            saveCaches(key, cluster, json, false);
            saveModels(key, cluster, json);
            saveIGFSs(key, cluster, json);
        });
    }

    /**
     * Save basic cluster.
     *
     * @param key Configuration key.
     * @param json Configuration in JSON format.
     */
    public void saveBasicCluster(ConfigurationKey key, JsonObject json) {
        txMgr.doInTransaction(() -> {
            Cluster cluster = saveCluster(key, json);

            saveCaches(key, cluster, json, true);
        });
    }

    /**
     * Delete objects that relates to cluster.
     *
     * @param clusterId Cluster ID.
     * @param fkIdx Foreign key.
     * @param tbl Table with children.
     */
    @SuppressWarnings("unchecked")
    private void deleteClusterObjects(ConfigurationKey key, UUID clusterId, OneToManyIndex fkIdx, Table<? extends DataObject> tbl) {
        Set<UUID> ids = fkIdx.delete(clusterId);

        cfgIdx.validateAll(key, ids);
        cfgIdx.removeAll(key, ids);

        tbl.deleteAll(ids);
    }

    /**
     * Delete all objects that relates to cluster.
     *
     * @param clusterId Cluster ID.
     */
    protected void deleteAllClusterObjects(ConfigurationKey key, UUID clusterId) {
        deleteClusterObjects(key, clusterId, cachesIdx, cachesTbl);
        deleteClusterObjects(key, clusterId, modelsIdx, modelsTbl);
    }

    /**
     * Delete clusters.
     *
     * @param key Configuration key.
     * @param clusterIds Cluster IDs to delete.
     */
    public void deleteClusters(ConfigurationKey key, TreeSet<UUID> clusterIds) {
        txMgr.doInTransaction(() -> {
            clustersIdx.validateAll(key, clusterIds);

            clusterIds.forEach(clusterId -> deleteAllClusterObjects(key, clusterId));

            clustersTbl.deleteAll(clusterIds);
            clustersIdx.removeAll(key, clusterIds);
        });
    }

    /**
     * Delete all configurations for specified account.
     *
     * @param key Configuration key.
     */
    public void deleteByAccountId(ConfigurationKey key) {
        txMgr.doInTransaction(() -> {
            Set<UUID> clusterIds = clustersIdx.delete(key);

            clusterIds.forEach(clusterId -> deleteAllClusterObjects(key, clusterId));

            clustersTbl.deleteAll(clusterIds);
        });
    }
}
