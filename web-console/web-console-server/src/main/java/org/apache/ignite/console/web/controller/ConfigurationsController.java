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

package org.apache.ignite.console.web.controller;

import java.util.UUID;
import io.swagger.v3.oas.annotations.Operation;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Cluster;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.apache.ignite.console.common.Utils.idsFromJson;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for configurations API.
 */
@RestController
@RequestMapping(path = "/api/v1/configuration")
public class ConfigurationsController {
    /** */
    private final ConfigurationsService cfgsSrv;

    /**
     * @param cfgsSrv Configurations service.
     */
    public ConfigurationsController(ConfigurationsService cfgsSrv) {
        this.cfgsSrv = cfgsSrv;
    }
    
    private UUID findClusterId(Account acc, boolean demo, String clusterId) {
    	try {
    		return UUID.fromString(clusterId);
    	}
    	catch(IllegalArgumentException e) {
    		
    		JsonArray list = cfgsSrv.loadClusters(new ConfigurationKey(acc.getId(), demo));
    		for(Object row: list) {
    			JsonObject cluster = (JsonObject)row;
    			String name = cluster.getString("name");
				if(clusterId.equals(name)) {
					return Cluster.getUUID(cluster, "id");
				}    			
    		}
    	}
    	throw new IllegalArgumentException("Not find clusteId "+ clusterId);
    }

    /**
     * @param acc Account.
     * @param clusterId Cluster ID.
     */
    @Operation(summary = "Get full cluster object.")
    @GetMapping(path = "/{clusterId}")
    public ResponseEntity<JsonObject> loadConfiguration(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @PathVariable("clusterId") String clusterId
    ) {
    	UUID clusterGUID = findClusterId(acc, demo, clusterId);
        return ResponseEntity.ok(cfgsSrv.loadConfiguration(new ConfigurationKey(acc.getId(), demo), clusterGUID));
    }

    /**
     * @param acc Account.
     * @return Clusters short list.
     */
    @Operation(summary = "Clusters short list.")
    @GetMapping(path = "/clusters")
    public ResponseEntity<JsonArray> loadClustersShortList(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo
    ) {
        return ResponseEntity.ok(cfgsSrv.loadClusters(new ConfigurationKey(acc.getId(), demo)));
    }

    /**
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Cluster as JSON.
     */
    @Operation(summary = "Get cluster configuration.")
    @GetMapping(path = "/clusters/{clusterId}")
    public ResponseEntity<String> loadCluster(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @PathVariable("clusterId") String clusterId
    ) {
    	UUID clusterGUID = findClusterId(acc, demo, clusterId);
        return ResponseEntity.ok(cfgsSrv.loadCluster(new ConfigurationKey(acc.getId(), demo), clusterGUID));
    }

    /**
     * Load cluster caches short list.
     *
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Caches short list.
     */
    @Operation(summary = "Caches short list.")
    @GetMapping(path = "/clusters/{clusterId}/caches")
    public ResponseEntity<JsonArray> loadCachesShortList(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @PathVariable("clusterId") String clusterId
    ) {
    	UUID clusterGUID = findClusterId(acc, demo, clusterId);
        return ResponseEntity.ok(cfgsSrv.loadShortCaches(new ConfigurationKey(acc.getId(), demo), clusterGUID));
    }

    /**
     * Load cluster models short list.
     *
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Models short list.
     */
    @Operation(summary = "Get models short list.")
    @GetMapping(path = "/clusters/{clusterId}/models")
    public ResponseEntity<JsonArray> loadModelsShortList(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @PathVariable("clusterId") String clusterId
    ) {
    	UUID clusterGUID = findClusterId(acc, demo, clusterId);
        return ResponseEntity.ok(cfgsSrv.loadShortModels(new ConfigurationKey(acc.getId(), demo), clusterGUID));
    }

    /**
     * @param acc Account.
     * @param cacheId Cache ID.
     */
    @Operation(summary = "Get cache configuration.")
    @GetMapping(path = "/caches/{cacheId}")
    public ResponseEntity<String> loadCache(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @PathVariable("cacheId") UUID cacheId
    ) {    	
        return ResponseEntity.ok(cfgsSrv.loadCache(new ConfigurationKey(acc.getId(), demo), cacheId));
    }

    /**
     * @param acc Account.
     * @param mdlId Model ID.
     */
    @Operation(summary = "Get model configuration.")
    @GetMapping(path = "/domains/{modelId}")
    public ResponseEntity<String> loadModel(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @PathVariable("modelId") UUID mdlId
    ) {
        return ResponseEntity.ok(cfgsSrv.loadModel(new ConfigurationKey(acc.getId(), demo), mdlId));
    }
    
    /**
     * @param acc Account.
     * @param {catalog}/{schema}/{table} Model Path.
     */
    @Operation(summary = "Get model configuration.")
    @GetMapping(path = "/domains/{catalog}/{schema}/{table}")
    public ResponseEntity<String> loadModelByName(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @PathVariable("catalog") String catalog,
        @PathVariable("schema") String schema,
        @PathVariable("table") String table
    ) {
        return ResponseEntity.ok(cfgsSrv.loadModel(new ConfigurationKey(acc.getId(), demo), catalog, schema, table));
    }

    /**
     * Save cluster.
     *
     * @param acc Account.
     * @param changedItems Items to save.
     */
    @Operation(summary = "Save cluster advanced configuration.")
    @PutMapping(path = "/clusters", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> saveAdvancedCluster(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @RequestBody JsonObject changedItems
    ) {
        cfgsSrv.saveAdvancedCluster(new ConfigurationKey(acc.getId(), demo), changedItems);

        return ResponseEntity.ok().build();
    }

    /**
     * Save basic clusters.
     *
     * @param acc Account.
     * @param changedItems Items to save.
     */
    @Operation(summary = "Save cluster basic configuration.")
    @PutMapping(path = "/clusters/basic", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> saveBasicCluster(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @RequestBody JsonObject changedItems
    ) {
        cfgsSrv.saveBasicCluster(new ConfigurationKey(acc.getId(), demo), changedItems);

        return ResponseEntity.ok().build();
    }

    /**
     * Delete clusters.
     *
     * @param acc Account.
     * @param clusterIDs Cluster IDs for removal.
     */
    @Operation(summary = "Delete cluster.")
    @PostMapping(path = "/clusters/remove", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> deleteClusters(
        @AuthenticationPrincipal Account acc,
        @RequestHeader(value = "demoMode", defaultValue = "false") boolean demo,
        @RequestBody JsonObject clusterIDs
    ) {
        cfgsSrv.deleteClusters(new ConfigurationKey(acc.getId(), demo), idsFromJson(clusterIDs, "clusterIDs"));

        return ResponseEntity.ok().build();
    }
}
