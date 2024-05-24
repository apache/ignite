/*
 * Copyright 2022 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.vertx.spi.cluster.ignite;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.cache.*;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.configuration.CacheConfiguration.*;

/**
 * @author Lukas Prettenthaler
 */
@DataObject(generateConverter = true)
public class IgniteCacheOptions {
  private String name;
  private CacheMode cacheMode;
  private int backups;
  private boolean readFromBackup;
  private CacheAtomicityMode atomicityMode;
  private CacheWriteSynchronizationMode writeSynchronizationMode;
  private boolean copyOnRead;
  private boolean eagerTtl;
  private boolean encryptionEnabled;
  private String groupName;
  private boolean invalidate;
  private int maxConcurrentAsyncOperations;
  private boolean onheapCacheEnabled;
  private PartitionLossPolicy partitionLossPolicy;
  private CacheRebalanceMode rebalanceMode;
  private int rebalanceOrder;
  private long rebalanceDelay;
  private int maxQueryInteratorsCount;
  private boolean eventsDisabled;
  private JsonObject expiryPolicy;
  private boolean metricsEnabled;

  /**
   * Default constructor
   */
  public IgniteCacheOptions() {
    atomicityMode = DFLT_CACHE_ATOMICITY_MODE;
    writeSynchronizationMode = PRIMARY_SYNC;
    cacheMode = DFLT_CACHE_MODE;
    backups = DFLT_BACKUPS;
    readFromBackup = DFLT_READ_FROM_BACKUP;
    copyOnRead = DFLT_COPY_ON_READ;
    eagerTtl = DFLT_EAGER_TTL;
    invalidate = DFLT_INVALIDATE;
    maxConcurrentAsyncOperations = DFLT_MAX_CONCURRENT_ASYNC_OPS;
    partitionLossPolicy = DFLT_PARTITION_LOSS_POLICY;
    rebalanceMode = DFLT_REBALANCE_MODE;
    maxQueryInteratorsCount = DFLT_MAX_QUERY_ITERATOR_CNT;
    eventsDisabled = DFLT_EVENTS_DISABLED;
    metricsEnabled = false;
  }

  /**
   * Copy constructor
   *
   * @param options the one to copy
   */
  public IgniteCacheOptions(IgniteCacheOptions options) {
    this.name = options.name;
    this.cacheMode = options.cacheMode;
    this.backups = options.backups;
    this.readFromBackup = options.readFromBackup;
    this.atomicityMode = options.atomicityMode;
    this.writeSynchronizationMode = options.writeSynchronizationMode;
    this.copyOnRead = options.copyOnRead;
    this.eagerTtl = options.eagerTtl;
    this.encryptionEnabled = options.encryptionEnabled;
    this.groupName = options.groupName;
    this.invalidate = options.invalidate;
    this.maxConcurrentAsyncOperations = options.maxConcurrentAsyncOperations;
    this.onheapCacheEnabled = options.onheapCacheEnabled;
    this.partitionLossPolicy = options.partitionLossPolicy;
    this.rebalanceMode = options.rebalanceMode;
    this.rebalanceOrder = options.rebalanceOrder;
    this.rebalanceDelay = options.rebalanceDelay;
    this.maxQueryInteratorsCount = options.maxQueryInteratorsCount;
    this.eventsDisabled = options.eventsDisabled;
    this.expiryPolicy = options.expiryPolicy;
    this.metricsEnabled = options.metricsEnabled;
  }

  /**
   * Constructor from JSON
   *
   * @param options the JSON
   */
  public IgniteCacheOptions(JsonObject options) {
    this();
    IgniteCacheOptionsConverter.fromJson(options, this);
  }

  /**
   * Cache name.
   *
   * @return Cache name.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets cache name.
   *
   * @param name Cache name. Can not be <tt>null</tt> or empty.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Gets caching mode to use. You can configure cache either to be local-only,
   * fully replicated, partitioned, or near. If not provided, PARTITIONED
   * mode will be used by default.
   *
   * @return Caching mode.
   */
  public String getCacheMode() {
    return cacheMode.name();
  }

  /**
   * Sets caching mode.
   *
   * @param cacheMode Caching mode.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setCacheMode(String cacheMode) {
    this.cacheMode = CacheMode.valueOf(cacheMode);
    return this;
  }

  /**
   * Gets number of nodes used to back up single partition for PARTITIONED cache.
   *
   * @return Number of backup nodes for one partition.
   */
  public int getBackups() {
    return backups;
  }

  /**
   * Sets number of nodes used to back up single partition for PARTITIONED cache.
   *
   * @param backups Number of backup nodes for one partition.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setBackups(int backups) {
    this.backups = backups;
    return this;
  }

  /**
   * Gets flag indicating whether data can be read from backup.
   * If {@code false} always get data from primary node (never from backup).
   *
   * @return {@code true} if data can be read from backup node or {@code false} if data always
   *      should be read from primary node and never from backup.
   */
  public boolean isReadFromBackup() {
    return readFromBackup;
  }

  /**
   * Sets read from backup flag.
   *
   * @param readFromBackup {@code true} to allow reads from backups.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setReadFromBackup(boolean readFromBackup) {
    this.readFromBackup = readFromBackup;
    return this;
  }

  /**
   * Gets cache atomicity mode.
   *
   * @return Cache atomicity mode.
   */
  public String getAtomicityMode() {
    return atomicityMode.name();
  }

  /**
   * Sets cache atomicity mode.
   *
   * @param atomicityMode Cache atomicity mode.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setAtomicityMode(String atomicityMode) {
    this.atomicityMode = CacheAtomicityMode.valueOf(atomicityMode);
    return this;
  }

  /**
   * Gets write synchronization mode. This mode controls whether the main
   * caller should wait for update on other nodes to complete or not.
   *
   * @return Write synchronization mode.
   */
  public String getWriteSynchronizationMode() {
    return writeSynchronizationMode.name();
  }

  /**
   * Sets write synchronization mode.
   *
   * @param writeSynchronizationMode Write synchronization mode.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setWriteSynchronizationMode(String writeSynchronizationMode) {
    this.writeSynchronizationMode = valueOf(writeSynchronizationMode);
    return this;
  }

  /**
   * Gets the flag indicating whether a copy of the value stored in the on-heap cache
   * should be created for a cache operation return the value.
   *
   * If the on-heap cache is disabled then this flag is of no use.
   *
   * @return Copy on read flag.
   */
  public boolean isCopyOnRead() {
    return copyOnRead;
  }

  /**
   * Sets copy on read flag.
   *
   * @param copyOnRead Copy on get flag.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setCopyOnRead(boolean copyOnRead) {
    this.copyOnRead = copyOnRead;
    return this;
  }

  /**
   * Gets flag indicating whether expired cache entries will be eagerly removed from cache.
   * If there is at least one cache configured with this flag set to {@code true}, Ignite
   * will create a single thread to clean up expired entries in background. When flag is
   * set to {@code false}, expired entries will be removed on next entry access.
   *
   * @return Flag indicating whether Ignite will eagerly remove expired entries.
   */
  public boolean isEagerTtl() {
    return eagerTtl;
  }

  /**
   * Sets eager ttl flag.
   *
   * @param eagerTtl {@code True} if Ignite should eagerly remove expired cache entries.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setEagerTtl(boolean eagerTtl) {
    this.eagerTtl = eagerTtl;
    return this;
  }

  /**
   * Gets flag indicating whether data must be encrypted.
   *
   * @return {@code True} if this cache persistent data is encrypted.
   */
  public boolean isEncryptionEnabled() {
    return encryptionEnabled;
  }

  /**
   * Sets encrypted flag.
   *
   * @param encryptionEnabled {@code True} if this cache persistent data should be encrypted.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setEncryptionEnabled(boolean encryptionEnabled) {
    this.encryptionEnabled = encryptionEnabled;
    return this;
  }

  /**
   * Gets the cache group name.
   *
   * Caches with the same group name share single underlying 'physical' cache (partition set),
   * but are logically isolated.
   *
   * Grouping caches reduces overall overhead, since internal data structures are shared.
   *
   * @return Cache group name.
   */
  public String getGroupName() {
    return groupName;
  }

  /**
   * Sets the cache group name.
   *
   * Caches with the same group name share single underlying 'physical' cache (partition set),
   * but are logically isolated.
   *
   * Grouping caches reduces overall overhead, since internal data structures are shared.
   *
   * @param groupName Cache group name.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  /**
   * Invalidation flag. If {@code true}, values will be invalidated (nullified) upon commit in near cache.
   *
   * @return Invalidation flag.
   */
  public boolean isInvalidate() {
    return invalidate;
  }

  /**
   * Sets invalidation flag for near cache entries in this transaction. Default is {@code false}.
   *
   * @param invalidate Flag to set this cache into invalidation-based mode. Default value is {@code false}.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setInvalidate(boolean invalidate) {
    this.invalidate = invalidate;
    return this;
  }

  /**
   * Gets maximum number of allowed concurrent asynchronous operations. If 0 returned then number
   * of concurrent asynchronous operations is unlimited.
   *
   * If user threads do not wait for asynchronous operations to complete, it is possible to overload
   * a system. This property enables back-pressure control by limiting number of scheduled asynchronous
   * cache operations.
   *
   * @return Maximum number of concurrent asynchronous operations or {@code 0} if unlimited.
   */
  public int getMaxConcurrentAsyncOperations() {
    return maxConcurrentAsyncOperations;
  }

  /**
   * Sets maximum number of concurrent asynchronous operations.
   *
   * @param maxConcurrentAsyncOperations Maximum number of concurrent asynchronous operations.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setMaxConcurrentAsyncOperations(int maxConcurrentAsyncOperations) {
    this.maxConcurrentAsyncOperations = maxConcurrentAsyncOperations;
    return this;
  }

  /**
   * Checks if the on-heap cache is enabled for the off-heap based page memory.
   *
   * @return On-heap cache enabled flag.
   */
  public boolean isOnheapCacheEnabled() {
    return onheapCacheEnabled;
  }

  /**
   * Configures on-heap cache for the off-heap based page memory.
   *
   * @param onheapCacheEnabled {@code True} if on-heap cache should be enabled.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setOnheapCacheEnabled(boolean onheapCacheEnabled) {
    this.onheapCacheEnabled = onheapCacheEnabled;
    return this;
  }

  /**
   * Gets partition loss policy. This policy defines how Ignite will react to a situation when all nodes for
   * some partition leave the cluster.
   *
   * @return Partition loss policy.
   */
  public String getPartitionLossPolicy() {
    return partitionLossPolicy.name();
  }

  /**
   * Sets partition loss policy. This policy defines how Ignite will react to a situation when all nodes for
   * some partition leave the cluster.
   *
   * @param partitionLossPolicy Partition loss policy.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setPartitionLossPolicy(String partitionLossPolicy) {
    this.partitionLossPolicy = PartitionLossPolicy.valueOf(partitionLossPolicy);
    return this;
  }

  /**
   * Gets rebalance mode for distributed cache.
   *
   * @return Rebalance mode.
   */
  public String getRebalanceMode() {
    return rebalanceMode.name();
  }

  /**
   * Sets cache rebalance mode.
   *
   * @param rebalanceMode Rebalance mode.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setRebalanceMode(String rebalanceMode) {
    this.rebalanceMode = CacheRebalanceMode.valueOf(rebalanceMode);
    return this;
  }

  /**
   * Gets cache rebalance order. Rebalance order can be set to non-zero value for caches with
   * {@link CacheRebalanceMode#SYNC SYNC} or {@link CacheRebalanceMode#ASYNC ASYNC} rebalance modes only.
   *
   * If cache rebalance order is positive, rebalancing for this cache will be started only when rebalancing for
   * all caches with smaller rebalance order will be completed.
   *
   * Note that cache with order {@code 0} does not participate in ordering. This means that cache with
   * rebalance order {@code 0} will never wait for any other caches. All caches with order {@code 0} will
   * be rebalanced right away concurrently with each other and ordered rebalance processes.
   *
   * If not set, cache order is 0, i.e. rebalancing is not ordered.
   *
   * @return Cache rebalance order.
   */
  public int getRebalanceOrder() {
    return rebalanceOrder;
  }

  /**
   * Sets cache rebalance order.
   *
   * @param rebalanceOrder Cache rebalance order.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setRebalanceOrder(int rebalanceOrder) {
    this.rebalanceOrder = rebalanceOrder;
    return this;
  }

  /**
   * Gets delay in milliseconds upon a node joining or leaving topology (or crash) after which rebalancing
   * should be started automatically. Rebalancing should be delayed if you plan to restart nodes
   * after they leave topology, or if you plan to start multiple nodes at once or one after another
   * and don't want to repartition and rebalance until all nodes are started.
   *
   * For better efficiency user should usually make sure that new nodes get placed on
   * the same place of consistent hash ring as the left nodes, and that nodes are
   * restarted before this delay expires. As an example,
   * node IP address and port combination may be used in this case.
   *
   * Default value is {@code 0} which means that repartitioning and rebalancing will start
   * immediately upon node leaving topology. If {@code -1} is returned, then rebalancing
   * will only be started manually by calling rebalance() method or from management console.
   *
   * @return Rebalancing delay, {@code 0} to start rebalancing immediately, {@code -1} to
   *      start rebalancing manually, or positive value to specify delay in milliseconds
   *      after which rebalancing should start automatically.
   * @deprecated Use baseline topology feature instead. Please, be aware this API will be removed in the next releases.
   */
  @Deprecated
  public long getRebalanceDelay() {
    return rebalanceDelay;
  }

  /**
   * Sets rebalance delay.
   *
   * @param rebalanceDelay Rebalance delay to set.
   * @return reference to this, for fluency
   * @deprecated Use baseline topology feature instead. Please, be aware this API will be removed in the next releases.
   */
  @Deprecated
  public IgniteCacheOptions setRebalanceDelay(long rebalanceDelay) {
    this.rebalanceDelay = rebalanceDelay;
    return this;
  }

  /**
   * Gets maximum number of query iterators that can be stored. Iterators are stored to
   * support query pagination when each page of data is sent to user's node only on demand.
   * Increase this property if you are running and processing lots of queries in parallel.
   *
   * @return Maximum number of query iterators that can be stored.
   */
  public int getMaxQueryInteratorsCount() {
    return maxQueryInteratorsCount;
  }

  /**
   * Sets maximum number of query iterators that can be stored.
   *
   * @param maxQueryInteratorsCount Maximum number of query iterators that can be stored.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setMaxQueryInteratorsCount(int maxQueryInteratorsCount) {
    this.maxQueryInteratorsCount = maxQueryInteratorsCount;
    return this;
  }

  /**
   * Checks whether events are disabled for this cache.
   *
   * @return Events disabled flag.
   */
  public boolean isEventsDisabled() {
    return eventsDisabled;
  }

  /**
   * Sets events disabled flag.
   *
   * @param eventsDisabled Events disabled flag.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setEventsDisabled(boolean eventsDisabled) {
    this.eventsDisabled = eventsDisabled;
    return this;
  }

  /**
   * Gets cache expiry policy object.
   *
   * @return Json representation of expiry policy.
   */
  public JsonObject getExpiryPolicy() {
    return expiryPolicy;
  }

  /**
   * Sets cache expiry policy object.
   * Requires a duration in milliseconds and an optional type which defaults to "created"
   * Valid type values are: accessed, modified, touched and created
   *
   * @param expiryPolicy Json representation of expiry policys.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setExpiryPolicy(JsonObject expiryPolicy) {
    this.expiryPolicy = expiryPolicy;
    return this;
  }

  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  /**
   * Sets cache metrics enabled/disabled.
   * Defaults to false
   *
   * @param metricsEnabled to set.
   * @return reference to this, for fluency
   */
  public IgniteCacheOptions setMetricsEnabled(boolean metricsEnabled) {
    this.metricsEnabled = metricsEnabled;
    return this;
  }

  /**
   * Convert to JSON
   *
   * @return the JSON
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    IgniteCacheOptionsConverter.toJson(this, json);
    return json;
  }
}
