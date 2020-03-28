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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.affinity.LocalAffinityFunction;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.query.QuerySchemaPatch;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.spi.indexing.noop.NoopIndexingSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CONSISTENCY_CHECK_SKIPPED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_TX_CONFIG;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isDefaultDataRegionPersistent;
import static org.apache.ignite.internal.processors.security.SecurityUtils.nodeSecurityContext;

/**
 * Util class for joining node validation.
 */
public class ValidationOnNodeJoinUtils {
    /** Template of message of conflicts during configuration merge */
    private static final String MERGE_OF_CONFIG_CONFLICTS_MESSAGE =
        "Conflicts during configuration merge for cache '%s' : \n%s";

    /** Template of message of node join was fail because it requires to merge of config */
    private static final String MERGE_OF_CONFIG_REQUIRED_MESSAGE = "Failed to join node to the active cluster " +
        "(the config of the cache '%s' has to be merged which is impossible on active grid). " +
        "Deactivate grid and retry node join or clean the joining node.";

    /** Template of message of failed node join because encryption settings are different for the same cache. */
    private static final String ENCRYPT_MISMATCH_MESSAGE = "Failed to join node to the cluster " +
        "(encryption settings are different for cache '%s' : local=%s, remote=%s.)";

    /** Supports non default precision and scale for DECIMAL and VARCHAR types. */
    private static final IgniteProductVersion PRECISION_SCALE_SINCE_VER = IgniteProductVersion.fromString("2.7.0");

    /** Invalid region configuration message. */
    private static final String INVALID_REGION_CONFIGURATION_MESSAGE = "Failed to join node " +
        "(Incompatible data region configuration [region=%s, locNodeId=%s, isPersistenceEnabled=%s, rmtNodeId=%s, isPersistenceEnabled=%s])";

    /**
     * Checks a joining node to configuration consistency.
     *
     * @param node Node.
     * @param discoData Disco data.
     * @param marsh Marsh.
     * @param ctx Context.
     * @param cacheDescProvider Cache descriptor provider.
     */
    @Nullable static IgniteNodeValidationResult validateNode(
        ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData,
        Marshaller marsh,
        GridKernalContext ctx,
        Function<String, DynamicCacheDescriptor> cacheDescProvider
    ) {
        if (discoData.hasJoiningNodeData() && discoData.joiningNodeData() instanceof CacheJoinNodeDiscoveryData) {
            CacheJoinNodeDiscoveryData nodeData = (CacheJoinNodeDiscoveryData)discoData.joiningNodeData();

            boolean isGridActive = ctx.state().clusterState().active();

            StringBuilder errorMsg = new StringBuilder();

            if (!node.isClient()) {
                validateRmtRegions(node, ctx).forEach(error -> {
                    if (errorMsg.length() > 0)
                        errorMsg.append("\n");

                    errorMsg.append(error);
                });
            }

            SecurityContext secCtx = null;

            if (ctx.security().enabled()) {
                try {
                    secCtx = nodeSecurityContext(marsh, U.resolveClassLoader(ctx.config()), node);
                }
                catch (SecurityException se) {
                    errorMsg.append(se.getMessage());
                }
            }

            for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : nodeData.caches().values()) {
                if (secCtx != null && cacheInfo.cacheType() == CacheType.USER) {
                    try (OperationSecurityContext s = ctx.security().withContext(secCtx)) {
                        GridCacheProcessor.authorizeCacheCreate(ctx.security(), cacheInfo.cacheData().config());
                    }
                    catch (SecurityException ex) {
                        if (errorMsg.length() > 0)
                            errorMsg.append("\n");

                        errorMsg.append(ex.getMessage());
                    }
                }

                DynamicCacheDescriptor locDesc = cacheDescProvider.apply(cacheInfo.cacheData().config().getName());

                if (locDesc == null)
                    continue;

                QuerySchemaPatch schemaPatch = locDesc.makeSchemaPatch(cacheInfo.cacheData().queryEntities());

                if (schemaPatch.hasConflicts() || (isGridActive && !schemaPatch.isEmpty())) {
                    if (errorMsg.length() > 0)
                        errorMsg.append("\n");

                    if (schemaPatch.hasConflicts())
                        errorMsg.append(String.format(MERGE_OF_CONFIG_CONFLICTS_MESSAGE,
                            locDesc.cacheName(), schemaPatch.getConflictsMessage()));
                    else
                        errorMsg.append(String.format(MERGE_OF_CONFIG_REQUIRED_MESSAGE, locDesc.cacheName()));
                }

                // This check must be done on join, otherwise group encryption key will be
                // written to metastore regardless of validation check and could trigger WAL write failures.
                boolean locEnc = locDesc.cacheConfiguration().isEncryptionEnabled();
                boolean rmtEnc = cacheInfo.cacheData().config().isEncryptionEnabled();

                if (locEnc != rmtEnc) {
                    if (errorMsg.length() > 0)
                        errorMsg.append("\n");

                    // Message will be printed on remote node, so need to swap local and remote.
                    errorMsg.append(String.format(ENCRYPT_MISMATCH_MESSAGE, locDesc.cacheName(), rmtEnc, locEnc));
                }
            }

            if (errorMsg.length() > 0) {
                String msg = errorMsg.toString();

                return new IgniteNodeValidationResult(node.id(), msg);
            }
        }
        return null;
    }

    /**
     * @param c Ignite configuration.
     * @param cc Configuration to validate.
     * @param cacheType Cache type.
     * @param cfgStore Cache store.
     * @param ctx Context.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    static void validate(
        IgniteConfiguration c,
        CacheConfiguration cc,
        CacheType cacheType,
        @Nullable CacheStore cfgStore,
        GridKernalContext ctx,
        IgniteLogger log,
        BiFunction<Boolean, String, IgniteCheckedException> assertParam
    ) throws IgniteCheckedException {
        apply(assertParam, cc.getName() != null && !cc.getName().isEmpty(), "name is null or empty");

        if (cc.getCacheMode() == REPLICATED) {
            if (cc.getNearConfiguration() != null &&
                ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName())) {
                U.warn(log, "Near cache cannot be used with REPLICATED cache, " +
                    "will be ignored [cacheName=" + U.maskName(cc.getName()) + ']');

                cc.setNearConfiguration(null);
            }
        }

        if (storesLocallyOnClient(c, cc, ctx))
            throw new IgniteCheckedException("DataRegion for client caches must be explicitly configured " +
                "on client node startup. Use DataStorageConfiguration to configure DataRegion.");

        if (cc.getCacheMode() == LOCAL && !cc.getAffinity().getClass().equals(LocalAffinityFunction.class))
            U.warn(log, "AffinityFunction configuration parameter will be ignored for local cache [cacheName=" +
                U.maskName(cc.getName()) + ']');

        if (cc.getAffinity().partitions() > CacheConfiguration.MAX_PARTITIONS_COUNT)
            throw new IgniteCheckedException("Cannot have more than " + CacheConfiguration.MAX_PARTITIONS_COUNT +
                " partitions [cacheName=" + cc.getName() + ", partitions=" + cc.getAffinity().partitions() + ']');

        if (cc.getRebalanceMode() != CacheRebalanceMode.NONE) {
            apply(assertParam, cc.getRebalanceBatchSize() > 0, "rebalanceBatchSize > 0");
            apply(assertParam, cc.getRebalanceTimeout() >= 0, "rebalanceTimeout >= 0");
            apply(assertParam, cc.getRebalanceThrottle() >= 0, "rebalanceThrottle >= 0");
            apply(assertParam, cc.getRebalanceBatchesPrefetchCount() > 0, "rebalanceBatchesPrefetchCount > 0");
        }

        if (cc.getCacheMode() == PARTITIONED || cc.getCacheMode() == REPLICATED) {
            if (cc.getAtomicityMode() == ATOMIC && cc.getWriteSynchronizationMode() == FULL_ASYNC)
                U.warn(log, "Cache write synchronization mode is set to FULL_ASYNC. All single-key 'put' and " +
                    "'remove' operations will return 'null', all 'putx' and 'removex' operations will return" +
                    " 'true' [cacheName=" + U.maskName(cc.getName()) + ']');
        }

        DeploymentMode depMode = c.getDeploymentMode();

        if (c.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !CU.isSystemCache(cc.getName()) && !(c.getMarshaller() instanceof BinaryMarshaller))
            throw new IgniteCheckedException("Cache can be started in PRIVATE or ISOLATED deployment mode only when" +
                " BinaryMarshaller is used [depMode=" + ctx.config().getDeploymentMode() + ", marshaller=" +
                c.getMarshaller().getClass().getName() + ']');

        if (cc.getAffinity().partitions() > CacheConfiguration.MAX_PARTITIONS_COUNT)
            throw new IgniteCheckedException("Affinity function must return at most " +
                CacheConfiguration.MAX_PARTITIONS_COUNT + " partitions [actual=" + cc.getAffinity().partitions() +
                ", affFunction=" + cc.getAffinity() + ", cacheName=" + cc.getName() + ']');

        if (cc.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
            apply(assertParam, cc.getCacheMode() != LOCAL,
                "LOCAL cache mode cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");

            apply(assertParam, cc.getNearConfiguration() == null,
                "near cache cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");

            apply(assertParam, !cc.isReadThrough(),
                "readThrough cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");

            apply(assertParam, !cc.isWriteThrough(),
                "writeThrough cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");

            apply(assertParam, !cc.isWriteBehindEnabled(),
                "writeBehindEnabled cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");

            apply(assertParam, cc.getRebalanceMode() != NONE,
                "Rebalance mode NONE cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");

            ExpiryPolicy expPlc = null;

            if (cc.getExpiryPolicyFactory() instanceof FactoryBuilder.SingletonFactory)
                expPlc = (ExpiryPolicy)cc.getExpiryPolicyFactory().create();

            if (!(expPlc instanceof EternalExpiryPolicy)) {
                apply(assertParam, cc.getExpiryPolicyFactory() == null,
                    "expiry policy cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");
            }

            apply(assertParam, cc.getInterceptor() == null,
                "interceptor cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");

            // Disable in-memory evictions for mvcc cache. TODO IGNITE-10738
            String memPlcName = cc.getDataRegionName();
            DataRegion dataRegion = ctx.cache().context().database().dataRegion(memPlcName);

            if (dataRegion != null && !dataRegion.config().isPersistenceEnabled() &&
                dataRegion.config().getPageEvictionMode() != DataPageEvictionMode.DISABLED) {
                throw new IgniteCheckedException("Data pages evictions cannot be used with TRANSACTIONAL_SNAPSHOT " +
                    "cache atomicity mode for in-memory regions. Please, either disable evictions or enable " +
                    "persistence for data regions with TRANSACTIONAL_SNAPSHOT caches. [cacheName=" + cc.getName() +
                    ", dataRegionName=" + memPlcName + ", pageEvictionMode=" +
                    dataRegion.config().getPageEvictionMode() + ']');
            }

            IndexingSpi idxSpi = ctx.config().getIndexingSpi();

            apply(assertParam, idxSpi == null || idxSpi instanceof NoopIndexingSpi,
                "Custom IndexingSpi cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode");
        }

        if (cc.isWriteBehindEnabled() && ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName())) {
            if (cfgStore == null)
                throw new IgniteCheckedException("Cannot enable write-behind (writer or store is not provided) " +
                    "for cache: " + U.maskName(cc.getName()));

            apply(assertParam, cc.getWriteBehindBatchSize() > 0, "writeBehindBatchSize > 0");
            apply(assertParam, cc.getWriteBehindFlushSize() >= 0, "writeBehindFlushSize >= 0");
            apply(assertParam, cc.getWriteBehindFlushFrequency() >= 0, "writeBehindFlushFrequency >= 0");
            apply(assertParam, cc.getWriteBehindFlushThreadCount() > 0, "writeBehindFlushThreadCount > 0");

            if (cc.getWriteBehindFlushSize() == 0 && cc.getWriteBehindFlushFrequency() == 0)
                throw new IgniteCheckedException("Cannot set both 'writeBehindFlushFrequency' and " +
                    "'writeBehindFlushSize' parameters to 0 for cache: " + U.maskName(cc.getName()));
        }

        if (cc.isReadThrough() && cfgStore == null
            && ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName()))
            throw new IgniteCheckedException("Cannot enable read-through (loader or store is not provided) " +
                "for cache: " + U.maskName(cc.getName()));

        if (cc.isWriteThrough() && cfgStore == null
            && ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName()))
            throw new IgniteCheckedException("Cannot enable write-through (writer or store is not provided) " +
                "for cache: " + U.maskName(cc.getName()));

        long delay = cc.getRebalanceDelay();

        if (delay != 0) {
            if (cc.getCacheMode() != PARTITIONED)
                U.warn(log, "Rebalance delay is supported only for partitioned caches (will ignore): " + (cc.getName()));
            else if (cc.getRebalanceMode() == SYNC) {
                if (delay < 0) {
                    U.warn(log, "Ignoring SYNC rebalance mode with manual rebalance start (node will not wait for " +
                        "rebalancing to be finished): " + U.maskName(cc.getName()));
                }
                else {
                    U.warn(log, "Using SYNC rebalance mode with rebalance delay (node will wait until rebalancing is " +
                        "initiated for " + delay + "ms) for cache: " + U.maskName(cc.getName()));
                }
            }
        }

        ctx.coordinators().validateCacheConfiguration(cc);

        if (cc.getAtomicityMode() == ATOMIC)
            apply(assertParam, cc.getTransactionManagerLookupClassName() == null,
                "transaction manager can not be used with ATOMIC cache");

        if ((cc.getEvictionPolicyFactory() != null || cc.getEvictionPolicy() != null) && !cc.isOnheapCacheEnabled())
            throw new IgniteCheckedException("Onheap cache must be enabled if eviction policy is configured [cacheName="
                + U.maskName(cc.getName()) + "]");

        if (cacheType != CacheType.DATA_STRUCTURES && DataStructuresProcessor.isDataStructureCache(cc.getName()))
            throw new IgniteCheckedException("Using cache names reserved for datastructures is not allowed for " +
                "other cache types [cacheName=" + cc.getName() + ", cacheType=" + cacheType + "]");

        if (cacheType != CacheType.DATA_STRUCTURES && DataStructuresProcessor.isReservedGroup(cc.getGroupName()))
            throw new IgniteCheckedException("Using cache group names reserved for datastructures is not allowed for " +
                "other cache types [cacheName=" + cc.getName() + ", groupName=" + cc.getGroupName() +
                ", cacheType=" + cacheType + "]");

        // Make sure we do not use sql schema for system views.
        if (ctx.query().moduleEnabled()) {
            String schema = QueryUtils.normalizeSchemaName(cc.getName(), cc.getSqlSchema());

            if (F.eq(schema, QueryUtils.SCHEMA_SYS)) {
                if (cc.getSqlSchema() == null) {
                    // Conflict on cache name.
                    throw new IgniteCheckedException("SQL schema name derived from cache name is reserved (" +
                        "please set explicit SQL schema name through CacheConfiguration.setSqlSchema() or choose " +
                        "another cache name) [cacheName=" + cc.getName() + ", schemaName=" + cc.getSqlSchema() + "]");
                }
                else {
                    // Conflict on schema name.
                    throw new IgniteCheckedException("SQL schema name is reserved (please choose another one) [" +
                        "cacheName=" + cc.getName() + ", schemaName=" + cc.getSqlSchema() + ']');
                }
            }
        }

        if (cc.isEncryptionEnabled() && !ctx.clientNode()) {
            StringBuilder cacheSpec = new StringBuilder("[cacheName=").append(cc.getName())
                .append(", groupName=").append(cc.getGroupName())
                .append(", cacheType=").append(cacheType)
                .append(']');

            if (!CU.isPersistentCache(cc, c.getDataStorageConfiguration())) {
                throw new IgniteCheckedException("Using encryption is not allowed" +
                    " for not persistent cache " + cacheSpec.toString());
            }

            EncryptionSpi encSpi = c.getEncryptionSpi();

            if (encSpi == null) {
                throw new IgniteCheckedException("EncryptionSpi should be configured to use encrypted cache " +
                    cacheSpec.toString());
            }

            if (cc.getDiskPageCompression() != DiskPageCompression.DISABLED)
                throw new IgniteCheckedException("Encryption cannot be used with disk page compression " +
                    cacheSpec.toString());
        }

        Collection<QueryEntity> ents = cc.getQueryEntities();

        if (ctx.discovery().discoCache() != null) {
            boolean nonDfltPrecScaleExists = ents.stream().anyMatch(
                e -> !F.isEmpty(e.getFieldsPrecision()) || !F.isEmpty(e.getFieldsScale()));

            if (nonDfltPrecScaleExists) {
                ClusterNode oldestNode = ctx.discovery().discoCache().oldestServerNode();

                if (PRECISION_SCALE_SINCE_VER.compareTo(oldestNode.version()) > 0) {
                    throw new IgniteCheckedException("Non default precision and scale is supported since version 2.7. " +
                        "The node with oldest version [node=" + oldestNode + ']');
                }
            }
        }
    }

    /**
     * @param ctx Context.
     * @param log Logger.
     * @throws IgniteCheckedException if check failed.
     */
    static void checkConsistency(GridKernalContext ctx, IgniteLogger log) throws IgniteCheckedException {
        Collection<ClusterNode> rmtNodes = ctx.discovery().remoteNodes();

        boolean changeablePoolSize =
            IgniteFeatures.allNodesSupports(rmtNodes, IgniteFeatures.DIFFERENT_REBALANCE_POOL_SIZE);

        for (ClusterNode n : rmtNodes) {
            if (Boolean.TRUE.equals(n.attribute(ATTR_CONSISTENCY_CHECK_SKIPPED)))
                continue;

            if (!changeablePoolSize)
                checkRebalanceConfiguration(n, ctx);

            checkTransactionConfiguration(n, ctx, log);

            checkMemoryConfiguration(n, ctx);

            DeploymentMode locDepMode = ctx.config().getDeploymentMode();
            DeploymentMode rmtDepMode = n.attribute(IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE);

            CU.checkAttributeMismatch(log, null, n.id(), "deploymentMode", "Deployment mode",
                locDepMode, rmtDepMode, true);
        }
    }

    /**
     * @param rmtNode Joining node.
     * @param ctx Context
     * @return List of validation errors.
     */
    private static List<String> validateRmtRegions(ClusterNode rmtNode, GridKernalContext ctx) {
        List<String> errorMessages = new ArrayList<>();

        DataStorageConfiguration rmtStorageCfg = extractDataStorage(rmtNode, ctx);
        Map<String, DataRegionConfiguration> rmtRegionCfgs = dataRegionCfgs(rmtStorageCfg);

        DataStorageConfiguration locStorageCfg = ctx.config().getDataStorageConfiguration();

        if (isDefaultDataRegionPersistent(locStorageCfg) != isDefaultDataRegionPersistent(rmtStorageCfg)) {
            errorMessages.add(String.format(
                INVALID_REGION_CONFIGURATION_MESSAGE,
                "DEFAULT",
                ctx.localNodeId(),
                isDefaultDataRegionPersistent(locStorageCfg),
                rmtNode.id(),
                isDefaultDataRegionPersistent(rmtStorageCfg)
            ));
        }

        for (ClusterNode clusterNode : ctx.discovery().aliveServerNodes()) {
            Map<String, DataRegionConfiguration> nodeRegionCfg = dataRegionCfgs(extractDataStorage(clusterNode, ctx));

            for (Map.Entry<String, DataRegionConfiguration> nodeRegionCfgEntry : nodeRegionCfg.entrySet()) {
                String regionName = nodeRegionCfgEntry.getKey();

                DataRegionConfiguration rmtRegionCfg = rmtRegionCfgs.get(regionName);

                if (rmtRegionCfg != null && rmtRegionCfg.isPersistenceEnabled() != nodeRegionCfgEntry.getValue().isPersistenceEnabled())
                    errorMessages.add(String.format(
                        INVALID_REGION_CONFIGURATION_MESSAGE,
                        regionName,
                        ctx.localNodeId(),
                        nodeRegionCfgEntry.getValue().isPersistenceEnabled(),
                        rmtNode.id(),
                        rmtRegionCfg.isPersistenceEnabled()
                    ));
            }
        }

        return errorMessages;
    }

    /**
     * @param assertParam Assert parameter.
     * @param cond The condition result.
     * @param condDesc The description of condition.
     */
    private static void apply(
        BiFunction<Boolean, String, IgniteCheckedException> assertParam,
        Boolean cond,
        String condDesc
    ) throws IgniteCheckedException {
        IgniteCheckedException apply = assertParam.apply(cond, condDesc);

        if (apply != null)
            throw apply;
    }

    /**
     * @param c Ignite Configuration.
     * @param cc Cache Configuration.
     * @param ctx Context.
     * @return {@code true} if cache is starting on client node and this node is affinity node for the cache.
     */
    private static boolean storesLocallyOnClient(IgniteConfiguration c, CacheConfiguration cc, GridKernalContext ctx) {
        if (c.isClientMode() && c.getDataStorageConfiguration() == null) {
            if (cc.getCacheMode() == LOCAL)
                return true;

            return ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName());

        }
        else
            return false;
    }

    /**
     * @param rmt Remote node to check.
     * @param ctx Context.
     * @throws IgniteCheckedException If check failed.
     */
    private static void checkRebalanceConfiguration(
        ClusterNode rmt,
        GridKernalContext ctx
    ) throws IgniteCheckedException {
        ClusterNode locNode = ctx.discovery().localNode();

        if (ctx.config().isClientMode() || locNode.isDaemon() || rmt.isClient() || rmt.isDaemon())
            return;

        Integer rebalanceThreadPoolSize = rmt.attribute(IgniteNodeAttributes.ATTR_REBALANCE_POOL_SIZE);

        if (rebalanceThreadPoolSize != null && rebalanceThreadPoolSize != ctx.config().getRebalanceThreadPoolSize()) {
            throw new IgniteCheckedException("Rebalance configuration mismatch (fix configuration or set -D" +
                IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system property)." +
                " Different values of such parameter may lead to rebalance process instability and hanging. " +
                " [rmtNodeId=" + rmt.id() +
                ", locRebalanceThreadPoolSize = " + ctx.config().getRebalanceThreadPoolSize() +
                ", rmtRebalanceThreadPoolSize = " + rebalanceThreadPoolSize + "]");
        }
    }

    /**
     * @param rmt Remote node to check.
     * @param ctx Context.
     * @param log Logger.
     * @throws IgniteCheckedException If check failed.
     */
    private static void checkTransactionConfiguration(
        ClusterNode rmt,
        GridKernalContext ctx,
        IgniteLogger log
    ) throws IgniteCheckedException {
        TransactionConfiguration rmtTxCfg = rmt.attribute(ATTR_TX_CONFIG);

        if (rmtTxCfg != null) {
            TransactionConfiguration locTxCfg = ctx.config().getTransactionConfiguration();

            checkDeadlockDetectionConfig(rmt, rmtTxCfg, locTxCfg, log);

            checkSerializableEnabledConfig(rmt, rmtTxCfg, locTxCfg);
        }
    }

    /**
     *
     */
    private static void checkDeadlockDetectionConfig(
        ClusterNode rmt,
        TransactionConfiguration rmtTxCfg,
        TransactionConfiguration locTxCfg,
        IgniteLogger log
    ) {
        boolean locDeadlockDetectionEnabled = locTxCfg.getDeadlockTimeout() > 0;
        boolean rmtDeadlockDetectionEnabled = rmtTxCfg.getDeadlockTimeout() > 0;

        if (locDeadlockDetectionEnabled != rmtDeadlockDetectionEnabled) {
            U.warn(log, "Deadlock detection is enabled on one node and disabled on another. " +
                "Disabled detection on one node can lead to undetected deadlocks. [rmtNodeId=" + rmt.id() +
                ", locDeadlockTimeout=" + locTxCfg.getDeadlockTimeout() +
                ", rmtDeadlockTimeout=" + rmtTxCfg.getDeadlockTimeout());
        }
    }

    /**
     *
     */
    private static void checkSerializableEnabledConfig(
        ClusterNode rmt,
        TransactionConfiguration rmtTxCfg,
        TransactionConfiguration locTxCfg
    ) throws IgniteCheckedException {
        if (locTxCfg.isTxSerializableEnabled() != rmtTxCfg.isTxSerializableEnabled())
            throw new IgniteCheckedException("Serializable transactions enabled mismatch " +
                "(fix txSerializableEnabled property or set -D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true " +
                "system property) [rmtNodeId=" + rmt.id() +
                ", locTxSerializableEnabled=" + locTxCfg.isTxSerializableEnabled() +
                ", rmtTxSerializableEnabled=" + rmtTxCfg.isTxSerializableEnabled() + ']');
    }

    /**
     * @param rmt Remote node to check.
     * @param ctx Context.
     * @throws IgniteCheckedException If check failed.
     */
    private static void checkMemoryConfiguration(ClusterNode rmt, GridKernalContext ctx) throws IgniteCheckedException {
        ClusterNode locNode = ctx.discovery().localNode();

        if (ctx.config().isClientMode() || locNode.isDaemon() || rmt.isClient() || rmt.isDaemon())
            return;

        DataStorageConfiguration dsCfg = null;

        Object dsCfgBytes = rmt.attribute(IgniteNodeAttributes.ATTR_DATA_STORAGE_CONFIG);

        if (dsCfgBytes instanceof byte[])
            dsCfg = new JdkMarshaller().unmarshal((byte[])dsCfgBytes, U.resolveClassLoader(ctx.config()));

        if (dsCfg == null) {
            // Try to use legacy memory configuration.
            MemoryConfiguration memCfg = rmt.attribute(IgniteNodeAttributes.ATTR_MEMORY_CONFIG);

            if (memCfg != null) {
                dsCfg = new DataStorageConfiguration();

                // All properties that are used in validation should be converted here.
                dsCfg.setPageSize(memCfg.getPageSize());
            }
        }

        if (dsCfg != null) {
            DataStorageConfiguration locDsCfg = ctx.config().getDataStorageConfiguration();

            if (dsCfg.getPageSize() != locDsCfg.getPageSize()) {
                throw new IgniteCheckedException("Memory configuration mismatch (fix configuration or set -D" +
                    IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system property) [rmtNodeId=" + rmt.id() +
                    ", locPageSize = " + locDsCfg.getPageSize() + ", rmtPageSize = " + dsCfg.getPageSize() + "]");
            }
        }
    }

    /**
     * @param node Joining node.
     * @param ctx Context.
     * @param map Cache descriptors.
     * @return Validation result or {@code null} in case of success.
     */
    @Nullable static IgniteNodeValidationResult validateHashIdResolvers(
        ClusterNode node,
        GridKernalContext ctx,
        Map<String, DynamicCacheDescriptor> map
    ) {
        if (!node.isClient()) {
            for (DynamicCacheDescriptor desc : map.values()) {
                CacheConfiguration cfg = desc.cacheConfiguration();

                if (cfg.getAffinity() instanceof RendezvousAffinityFunction) {
                    RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cfg.getAffinity();

                    Object nodeHashObj = aff.resolveNodeHash(node);

                    for (ClusterNode topNode : ctx.discovery().aliveServerNodes()) {
                        Object topNodeHashObj = aff.resolveNodeHash(topNode);

                        if (nodeHashObj.hashCode() == topNodeHashObj.hashCode()) {
                            String errMsg = "Failed to add node to topology because it has the same hash code for " +
                                "partitioned affinity as one of existing nodes [cacheName=" +
                                cfg.getName() + ", existingNodeId=" + topNode.id() + ']';

                            String sndMsg = "Failed to add node to topology because it has the same hash code for " +
                                "partitioned affinity as one of existing nodes [cacheName=" +
                                cfg.getName() + ", existingNodeId=" + topNode.id() + ']';

                            return new IgniteNodeValidationResult(topNode.id(), errMsg, sndMsg);
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * @param rmtNode Remote node to check.
     * @param ctx Context.
     * @return Data storage configuration
     */
    private static DataStorageConfiguration extractDataStorage(ClusterNode rmtNode, GridKernalContext ctx) {
        return GridCacheUtils.extractDataStorage(
            rmtNode,
            ctx.marshallerContext().jdkMarshaller(),
            U.resolveClassLoader(ctx.config())
        );
    }

    /**
     * @param dataStorageCfg User-defined data regions.
     */
    private static Map<String, DataRegionConfiguration> dataRegionCfgs(DataStorageConfiguration dataStorageCfg) {
        if (dataStorageCfg != null) {
            return Optional.ofNullable(dataStorageCfg.getDataRegionConfigurations())
                .map(Stream::of)
                .orElseGet(Stream::empty)
                .collect(Collectors.toMap(DataRegionConfiguration::getName, e -> e));
        }

        return Collections.emptyMap();
    }
}
