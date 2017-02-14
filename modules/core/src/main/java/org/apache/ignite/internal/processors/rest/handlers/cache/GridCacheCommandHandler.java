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

package org.apache.ignite.internal.processors.rest.handlers.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.GridClosureCallMode.BALANCE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_DECREMENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.ATOMIC_INCREMENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_ADD;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_APPEND;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_CAS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_CONTAINS_KEY;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_CONTAINS_KEYS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_AND_PUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_AND_PUT_IF_ABSENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_AND_REMOVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_GET_AND_REPLACE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_METADATA;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_METRICS;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PREPEND;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_PUT_IF_ABSENT;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REMOVE_VALUE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REPLACE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_REPLACE_VALUE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_SIZE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.DESTROY_CACHE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.GET_OR_CREATE_CACHE;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_NO_FAILOVER;

/**
 * Command handler for API requests.
 */
public class GridCacheCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(
        DESTROY_CACHE,
        GET_OR_CREATE_CACHE,
        CACHE_CONTAINS_KEYS,
        CACHE_CONTAINS_KEY,
        CACHE_GET,
        CACHE_GET_AND_PUT,
        CACHE_GET_AND_REPLACE,
        CACHE_GET_AND_PUT_IF_ABSENT,
        CACHE_PUT_IF_ABSENT,
        CACHE_GET_ALL,
        CACHE_PUT,
        CACHE_ADD,
        CACHE_PUT_ALL,
        CACHE_REMOVE,
        CACHE_REMOVE_VALUE,
        CACHE_REPLACE_VALUE,
        CACHE_GET_AND_REMOVE,
        CACHE_REMOVE_ALL,
        CACHE_REPLACE,
        CACHE_CAS,
        CACHE_APPEND,
        CACHE_PREPEND,
        CACHE_METRICS,
        CACHE_SIZE,
        CACHE_METADATA
    );

    /** Requests with required parameter {@code key}. */
    private static final EnumSet<GridRestCommand> KEY_REQUIRED_REQUESTS = EnumSet.of(
        CACHE_CONTAINS_KEY,
        CACHE_GET,
        CACHE_GET_AND_PUT,
        CACHE_GET_AND_REPLACE,
        CACHE_GET_AND_PUT_IF_ABSENT,
        CACHE_PUT_IF_ABSENT,
        CACHE_PUT,
        CACHE_ADD,
        CACHE_REMOVE,
        CACHE_REMOVE_VALUE,
        CACHE_REPLACE_VALUE,
        CACHE_GET_AND_REMOVE,
        CACHE_REPLACE,
        ATOMIC_INCREMENT,
        ATOMIC_DECREMENT,
        CACHE_CAS,
        CACHE_APPEND,
        CACHE_PREPEND
    );

    /**
     * @param ctx Context.
     */
    public GridCacheCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Retrieves cache flags from corresponding bits.
     *
     * @param cacheFlagsBits Integer representation of cache flags bit set.
     * @return Skip store flag.
     */
    public static boolean parseCacheFlags(int cacheFlagsBits) {
        if (cacheFlagsBits == 0)
            return false;

        if ((cacheFlagsBits & 1) != 0)
            return true;

        return false;
    }

    /**
     * Handles append and prepend commands.
     *
     * @param ctx Kernal context.
     * @param cache Cache.
     * @param key Key.
     * @param req Request.
     * @param prepend Whether to prepend.
     * @return Future of operation result.
     * @throws IgniteCheckedException In case of any exception.
     */
    private static IgniteInternalFuture<?> appendOrPrepend(
        final GridKernalContext ctx,
        final IgniteInternalCache<Object, Object> cache,
        final Object key, GridRestCacheRequest req,
        final boolean prepend) throws IgniteCheckedException {
        assert cache != null;
        assert key != null;
        assert req != null;

        final Object val = req.value();

        if (val == null)
            throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("val"));

        return ctx.closure().callLocalSafe(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                EntryProcessorResult<Boolean> res = cache.invoke(key, new EntryProcessor<Object, Object, Boolean>() {
                    @Override
                    public Boolean process(MutableEntry<Object, Object> entry,
                        Object... objects) throws EntryProcessorException {
                        try {
                            Object curVal = entry.getValue();

                            if (curVal == null)
                                return false;

                            // Modify current value with appendix one.
                            Object newVal = appendOrPrepend(curVal, val, !prepend);

                            // Put new value asynchronously.
                            entry.setValue(newVal);

                            return true;
                        }
                        catch (IgniteCheckedException e) {
                            throw new EntryProcessorException(e);
                        }
                    }
                });

                try {
                    return res.get();
                }
                catch (EntryProcessorException e) {
                    throw new IgniteCheckedException(e.getCause());
                }
            }
        }, false);
    }

    /**
     * Append or prepend new value to the current one.
     *
     * @param origVal Original value.
     * @param appendVal Appendix value to add to the original one.
     * @param appendPlc Append or prepend policy flag.
     * @return Resulting value.
     * @throws IgniteCheckedException In case of grid exceptions.
     */
    private static Object appendOrPrepend(Object origVal, Object appendVal,
        boolean appendPlc) throws IgniteCheckedException {
        // Strings.
        if (appendVal instanceof String && origVal instanceof String)
            return appendPlc ? origVal + (String)appendVal : (String)appendVal + origVal;

        // Maps.
        if (appendVal instanceof Map && origVal instanceof Map) {
            Map<Object, Object> origMap = (Map<Object, Object>)origVal;
            Map<Object, Object> appendMap = (Map<Object, Object>)appendVal;

            Map<Object, Object> map = X.cloneObject(origMap, false, true);

            if (appendPlc)
                map.putAll(appendMap); // Append.
            else {
                map.clear();
                map.putAll(appendMap); // Prepend.
                map.putAll(origMap);
            }

            for (Map.Entry<Object, Object> e : appendMap.entrySet()) // Remove zero-valued entries.
                if (e.getValue() == null && map.get(e.getKey()) == null)
                    map.remove(e.getKey());

            return map;
        }

        // Generic collection.
        if (appendVal instanceof Collection<?> && origVal instanceof Collection<?>) {
            Collection<Object> origCol = (Collection<Object>)origVal;
            Collection<Object> appendCol = (Collection<Object>)appendVal;

            Collection<Object> col = X.cloneObject(origCol, false, true);

            if (appendPlc)
                col.addAll(appendCol); // Append.
            else {
                col.clear();
                col.addAll(appendCol); // Prepend.
                col.addAll(origCol);
            }

            return col;
        }

        throw new IgniteCheckedException("Incompatible types [appendVal=" + appendVal + ", old=" + origVal + ']');
    }

    /**
     * Creates a transformation function from {@link CacheCommand}'s results into {@link GridRestResponse}.
     *
     * @param c Cache instance to obtain affinity data.
     * @param key Affinity key for previous operation.
     * @return Rest response.
     */
    private static IgniteClosure<IgniteInternalFuture<?>, GridRestResponse> resultWrapper(
        final IgniteInternalCache<Object, Object> c, @Nullable final Object key) {
        return new CX1<IgniteInternalFuture<?>, GridRestResponse>() {
            @Override public GridRestResponse applyx(IgniteInternalFuture<?> f) throws IgniteCheckedException {
                GridCacheRestResponse resp = new GridCacheRestResponse();

                resp.setResponse(f.get());

                if (key != null)
                    resp.setAffinityNodeId(c.cache().affinity().mapKeyToNode(key).id().toString());

                return resp;
            }
        };
    }

    /**
     * @param ignite Grid instance.
     * @param cacheName Name of the cache.
     * @return Instance on the named cache.
     * @throws IgniteCheckedException If cache not found.
     */
    private static IgniteInternalCache<Object, Object> cache(Ignite ignite,
        String cacheName) throws IgniteCheckedException {
        IgniteInternalCache<Object, Object> cache = ((IgniteKernal)ignite).getCache(cacheName);

        if (cache == null)
            throw new IgniteCheckedException(
                "Failed to find cache for given cache name (null for default cache): " + cacheName);

        return cache;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(final GridRestRequest req) {
        assert req instanceof GridRestCacheRequest : "Invalid command for topology handler: " + req;

        assert SUPPORTED_COMMANDS.contains(req.command());

        if (log.isDebugEnabled())
            log.debug("Handling cache REST request: " + req);

        GridRestCacheRequest req0 = (GridRestCacheRequest)req;

        final String cacheName = req0.cacheName();

        final Object key = req0.key();

        final boolean skipStore = parseCacheFlags(req0.cacheFlags());

        try {
            GridRestCommand cmd = req0.command();

            if (key == null && KEY_REQUIRED_REQUESTS.contains(cmd))
                throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("key"));

            final Long ttl = req0.ttl();

            IgniteInternalFuture<GridRestResponse> fut;

            switch (cmd) {
                case DESTROY_CACHE: {
                    // Do not check thread tx here since there can be active system cache txs.
                    fut = ((IgniteKernal)ctx.grid()).destroyCacheAsync(cacheName, false).chain(
                        new CX1<IgniteInternalFuture<?>, GridRestResponse>() {
                            @Override public GridRestResponse applyx(IgniteInternalFuture<?> f)
                                throws IgniteCheckedException {
                                return new GridRestResponse(f.get());
                            }
                        });

                    break;
                }

                case GET_OR_CREATE_CACHE: {
                    // Do not check thread tx here since there can be active system cache txs.
                    fut = ((IgniteKernal)ctx.grid()).getOrCreateCacheAsync(cacheName, false).chain(
                        new CX1<IgniteInternalFuture<?>, GridRestResponse>() {
                            @Override public GridRestResponse applyx(IgniteInternalFuture<?> f)
                                throws IgniteCheckedException {
                                return new GridRestResponse(f.get());
                            }
                        });

                    break;
                }

                case CACHE_METADATA: {
                    fut = ctx.task().execute(MetadataTask.class, null);

                    break;
                }

                case CACHE_CONTAINS_KEYS: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new ContainsKeysCommand(getKeys(req0)));

                    break;
                }

                case CACHE_CONTAINS_KEY: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new ContainsKeyCommand(key));

                    break;
                }

                case CACHE_GET: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new GetCommand(key));

                    break;
                }

                case CACHE_GET_AND_PUT: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new GetAndPutCommand(key, getValue(req0)));

                    break;
                }

                case CACHE_GET_AND_REPLACE: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new GetAndReplaceCommand(key, getValue(req0)));

                    break;
                }

                case CACHE_GET_AND_PUT_IF_ABSENT: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new GetAndPutIfAbsentCommand(key, getValue(req0)));

                    break;
                }

                case CACHE_PUT_IF_ABSENT: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new PutIfAbsentCommand(key, getValue(req0)));

                    break;
                }

                case CACHE_GET_ALL: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new GetAllCommand(getKeys(req0)));

                    break;
                }

                case CACHE_PUT: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key, new
                        PutCommand(key, ttl, getValue(req0)));

                    break;
                }

                case CACHE_ADD: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new AddCommand(key, ttl, getValue(req0)));

                    break;
                }

                case CACHE_PUT_ALL: {
                    Map<Object, Object> map = req0.values();

                    if (F.isEmpty(map))
                        throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("values"));

                    for (Map.Entry<Object, Object> e : map.entrySet()) {
                        if (e.getKey() == null)
                            throw new IgniteCheckedException("Failing putAll operation (null keys are not allowed).");

                        if (e.getValue() == null)
                            throw new IgniteCheckedException("Failing putAll operation (null values are not allowed).");
                    }

                    // HashMap wrapping for correct serialization
                    map = new HashMap<>(map);

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new PutAllCommand(map));

                    break;
                }

                case CACHE_REMOVE: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new RemoveCommand(key));

                    break;
                }

                case CACHE_REMOVE_VALUE: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new RemoveValueCommand(key, getValue(req0)));

                    break;
                }

                case CACHE_REPLACE_VALUE: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new ReplaceValueCommand(key, getValue(req0), req0.value2()));

                    break;
                }

                case CACHE_GET_AND_REMOVE: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new GetAndRemoveCommand(key));

                    break;
                }

                case CACHE_REMOVE_ALL: {
                    Map<Object, Object> map = req0.values();

                    // HashSet wrapping for correct serialization
                    Set<Object> keys = map == null ? null : new HashSet<>(map.keySet());

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new RemoveAllCommand(keys));

                    break;
                }

                case CACHE_REPLACE: {
                    final Object val = req0.value();

                    if (val == null)
                        throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("val"));

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new ReplaceCommand(key, ttl, val));

                    break;
                }

                case CACHE_CAS: {
                    final Object val1 = req0.value();
                    final Object val2 = req0.value2();

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new CasCommand(val2, val1, key));

                    break;
                }

                case CACHE_APPEND: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new AppendCommand(key, req0));

                    break;
                }

                case CACHE_PREPEND: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, skipStore, key,
                        new PrependCommand(key, req0));

                    break;
                }

                case CACHE_METRICS: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, key, new MetricsCommand());

                    break;
                }

                case CACHE_SIZE: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, key, new SizeCommand());

                    break;
                }

                default:
                    throw new IllegalArgumentException("Invalid command for cache handler: " + req);
            }

            return fut;
        }
        catch (IgniteException e) {
            U.error(log, "Failed to execute cache command: " + req, e);

            return new GridFinishedFuture<>(e);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to execute cache command: " + req, e);

            return new GridFinishedFuture<>(e);
        }
        finally {
            if (log.isDebugEnabled())
                log.debug("Handled cache REST request: " + req);
        }
    }

    /**
     * @param req Request.
     * @return Request keys.
     * @throws IgniteCheckedException If incorrect keys are presented.
     */
    private Set<Object> getKeys(GridRestCacheRequest req) throws IgniteCheckedException {
        Set<Object> keys = req.values().keySet();

        if (F.isEmpty(keys))
            throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("keys"));

        // HashSet wrapping for correct serialization
        HashSet<Object> keys0 = new HashSet<>();

        for (Object getKey : keys) {
            if (getKey == null)
                throw new IgniteCheckedException("Failing operation (null keys are not allowed).");

            keys0.add(getKey);
        }

        return keys0;
    }

    /**
     * @param req Request.
     * @return Request value.
     * @throws IgniteCheckedException If incorrect keys are presented.
     */
    private Object getValue(GridRestCacheRequest req) throws IgniteCheckedException {
        final Object val = req.value();

        if (val == null)
            throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("val"));

        return val;
    }

    /**
     * Executes command on flagged cache projection. Checks {@code destId} to find if command could be performed locally
     * or routed to a remote node.
     *
     * @param destId Target node Id for the operation. If {@code null} - operation could be executed anywhere.
     * @param clientId Client ID.
     * @param cacheName Cache name.
     * @param skipStore Skip store.
     * @param key Key to set affinity mapping in the response.
     * @param op Operation to perform.
     * @return Operation result in future.
     * @throws IgniteCheckedException If failed
     */
    private IgniteInternalFuture<GridRestResponse> executeCommand(
        @Nullable UUID destId,
        UUID clientId,
        final String cacheName,
        final boolean skipStore,
        final Object key,
        final CacheProjectionCommand op) throws IgniteCheckedException {

        final boolean locExec =
            destId == null || destId.equals(ctx.localNodeId()) || replicatedCacheAvailable(cacheName);

        if (locExec) {
            IgniteInternalCache<?, ?> prj = localCache(cacheName).forSubjectId(clientId).setSkipStore(skipStore);

            return op.apply((IgniteInternalCache<Object, Object>)prj, ctx).
                chain(resultWrapper((IgniteInternalCache<Object, Object>)prj, key));
        }
        else {
            ClusterGroup prj = ctx.grid().cluster().forPredicate(F.nodeForNodeId(destId));

            ctx.task().setThreadContext(TC_NO_FAILOVER, true);

            return ctx.closure().callAsync(BALANCE,
                new FlaggedCacheOperationCallable(clientId, cacheName, skipStore, op, key),
                prj.nodes());
        }
    }

    /**
     * Executes command on cache. Checks {@code destId} to find if command could be performed locally or routed to a
     * remote node.
     *
     * @param destId Target node Id for the operation. If {@code null} - operation could be executed anywhere.
     * @param clientId Client ID.
     * @param cacheName Cache name.
     * @param key Key to set affinity mapping in the response.
     * @param op Operation to perform.
     * @return Operation result in future.
     * @throws IgniteCheckedException If failed
     */
    private IgniteInternalFuture<GridRestResponse> executeCommand(
        @Nullable UUID destId,
        UUID clientId,
        final String cacheName,
        final Object key,
        final CacheCommand op) throws IgniteCheckedException {
        final boolean locExec = destId == null || destId.equals(ctx.localNodeId()) ||
            ctx.cache().cache(cacheName) != null;

        if (locExec) {
            final IgniteInternalCache<Object, Object> cache = localCache(cacheName).forSubjectId(clientId);

            return op.apply(cache, ctx).chain(resultWrapper(cache, key));
        }
        else {
            ClusterGroup prj = ctx.grid().cluster().forPredicate(F.nodeForNodeId(destId));

            ctx.task().setThreadContext(TC_NO_FAILOVER, true);

            return ctx.closure().callAsync(BALANCE,
                new CacheOperationCallable(clientId, cacheName, op, key),
                prj.nodes());
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheCommandHandler.class, this);
    }

    /**
     * @param cacheName Cache name.
     * @return If replicated cache with given name is locally available.
     */
    private boolean replicatedCacheAvailable(String cacheName) {
        GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);

        return cache != null && cache.configuration().getCacheMode() == CacheMode.REPLICATED;
    }

    /**
     * @param cacheName Name of the cache.
     * @return Instance on the named cache.
     * @throws IgniteCheckedException If cache not found.
     */
    protected IgniteInternalCache<Object, Object> localCache(String cacheName) throws IgniteCheckedException {
        IgniteInternalCache<Object, Object> cache = ctx.cache().cache(cacheName);

        if (cache == null)
            throw new IgniteCheckedException(
                "Failed to find cache for given cache name (null for default cache): " + cacheName);

        return cache;
    }

    /**
     * Fixed result closure.
     */
    private static final class FixedResult extends CX1<IgniteInternalFuture<?>, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Closure result. */
        private final Object res;

        /**
         * @param res Closure result.
         */
        private FixedResult(Object res) {
            this.res = res;
        }

        /** {@inheritDoc} */
        @Override public Object applyx(IgniteInternalFuture<?> f) throws IgniteCheckedException {
            f.get();

            return res;
        }
    }

    /**
     * Type alias.
     */
    private abstract static class CacheCommand
        extends IgniteClosure2X<IgniteInternalCache<Object, Object>, GridKernalContext, IgniteInternalFuture<?>> {
        /** */
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     * Type alias.
     */
    private abstract static class CacheProjectionCommand
        extends IgniteClosure2X<IgniteInternalCache<Object, Object>, GridKernalContext, IgniteInternalFuture<?>> {
        /** */
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     * Class for flagged cache operations.
     */
    @GridInternal
    private static class FlaggedCacheOperationCallable implements Callable<GridRestResponse>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;
        /** */
        private final String cacheName;
        /** */
        private final boolean skipStore;
        /** */
        private final CacheProjectionCommand op;
        /** */
        private final Object key;
        /** Client ID. */
        private UUID clientId;
        /** */
        @IgniteInstanceResource
        private Ignite g;

        /**
         * @param clientId Client ID.
         * @param cacheName Cache name.
         * @param skipStore Skip store.
         * @param op Operation.
         * @param key Key.
         */
        private FlaggedCacheOperationCallable(UUID clientId,
            String cacheName,
            boolean skipStore,
            CacheProjectionCommand op,
            Object key) {
            this.clientId = clientId;
            this.cacheName = cacheName;
            this.skipStore = skipStore;
            this.op = op;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            IgniteInternalCache<?, ?> prj = cache(g, cacheName).forSubjectId(clientId).setSkipStore(skipStore);

            // Need to apply both operation and response transformation remotely
            // as cache could be inaccessible on local node and
            // exception processing should be consistent with local execution.
            return op.apply((IgniteInternalCache<Object, Object>)prj, ((IgniteKernal)g).context()).
                chain(resultWrapper((IgniteInternalCache<Object, Object>)prj, key)).get();
        }
    }

    /**
     * Class for cache operations.
     */
    @GridInternal
    private static class CacheOperationCallable implements Callable<GridRestResponse>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;
        /** */
        private final String cacheName;
        /** */
        private final CacheCommand op;
        /** */
        private final Object key;
        /** Client ID. */
        private UUID clientId;
        /** */
        @IgniteInstanceResource
        private Ignite g;

        /**
         * @param clientId Client ID.
         * @param cacheName Cache name.
         * @param op Operation.
         * @param key Key.
         */
        private CacheOperationCallable(UUID clientId, String cacheName, CacheCommand op, Object key) {
            this.clientId = clientId;
            this.cacheName = cacheName;
            this.op = op;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            final IgniteInternalCache<Object, Object> cache = cache(g, cacheName).forSubjectId(clientId);

            // Need to apply both operation and response transformation remotely
            // as cache could be inaccessible on local node and
            // exception processing should be consistent with local execution.
            return op.apply(cache, ((IgniteKernal)g).context()).chain(resultWrapper(cache, key)).get();
        }
    }

    /** */
    private static class ContainsKeyCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /**
         * @param key Key.
         */
        ContainsKeyCommand(Object key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.containsKeyAsync(key);
        }
    }

    /** */
    @GridInternal
    private static class MetadataTask extends ComputeTaskAdapter<Void, GridRestResponse> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Void arg) throws IgniteException {

            GridDiscoveryManager discovery = ignite.context().discovery();

            boolean sameCaches = true;

            int hash = discovery.nodeCaches(F.first(subgrid)).hashCode();

            for (int i = 1; i < subgrid.size(); i++) {
                if (hash != discovery.nodeCaches(subgrid.get(i)).hashCode()) {
                    sameCaches = false;

                    break;
                }
            }

            Map<ComputeJob, ClusterNode> map = U.newHashMap(sameCaches ? 1 : subgrid.size());

            if (sameCaches)
                map.put(new MetadataJob(), ignite.localNode());
            else {
                for (ClusterNode node : subgrid)
                    map.put(new MetadataJob(), node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Nullable @Override public GridRestResponse reduce(List<ComputeJobResult> results) throws IgniteException {
            Map<String, GridCacheSqlMetadata> map = new HashMap<>();

            for (ComputeJobResult r : results) {
                if (!r.isCancelled() && r.getException() == null) {
                    for (GridCacheSqlMetadata m : r.<Collection<GridCacheSqlMetadata>>getData()) {
                        if (!map.containsKey(m.cacheName()))
                            map.put(m.cacheName(), m);
                    }
                }
            }

            Collection<GridCacheSqlMetadata> metas = new ArrayList<>(map.size());

            metas.addAll(map.values());

            return new GridRestResponse(metas);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetadataTask.class, this);
        }
    }

    /** */
    private static class MetadataJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Collection<GridCacheSqlMetadata> execute() {
            IgniteCacheProxy<?, ?> cache = F.first(ignite.context().cache().publicCaches());

            if (cache == null)
                return Collections.emptyList();

            try {
                return cache.context().queries().sqlMetadata();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetadataJob.class, this);
        }
    }

    /** */
    private static class ContainsKeysCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Collection<Object> keys;

        /**
         * @param keys Keys.
         */
        ContainsKeysCommand(Collection<Object> keys) {
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.containsKeysAsync(keys);
        }
    }

    /** */
    private static class GetCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /**
         * @param key Key.
         */
        GetCommand(Object key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.getAsync(key);
        }
    }

    /** */
    private static class GetAndPutCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** Key. */
        protected final Object key;

        /** Value. */
        protected final Object val;

        /**
         * @param key Key.
         * @param val Value.
         */
        GetAndPutCommand(Object key, Object val) {
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.getAndPutAsync(key, val);
        }
    }

    /** */
    private static class GetAndReplaceCommand extends GetAndPutCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param key Key.
         * @param val Value.
         */
        GetAndReplaceCommand(Object key, Object val) {
            super(key, val);
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.getAndReplaceAsync(key, val);
        }
    }

    /** */
    private static class ReplaceValueCommand extends GetAndReplaceCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object oldVal;

        /**
         * @param key Key.
         * @param val Value.
         * @param oldVal Old value.
         */
        ReplaceValueCommand(Object key, Object val, Object oldVal) {
            super(key, val);
            this.oldVal = oldVal;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.replaceAsync(key, oldVal, val);
        }
    }

    /** */
    private static class GetAndPutIfAbsentCommand extends GetAndPutCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param key Key.
         * @param val Value.
         */
        GetAndPutIfAbsentCommand(Object key, Object val) {
            super(key, val);
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.getAndPutIfAbsentAsync(key, val);
        }
    }

    /** */
    private static class PutIfAbsentCommand extends GetAndPutCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param key Key.
         * @param val Value.
         */
        PutIfAbsentCommand(Object key, Object val) {
            super(key, val);
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.putIfAbsentAsync(key, val);
        }
    }

    /** */
    private static class GetAllCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Collection<Object> keys;

        /**
         * @param keys Keys.
         */
        GetAllCommand(Collection<Object> keys) {
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.getAllAsync(keys);
        }
    }

    /** */
    private static class PutAllCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Map<Object, Object> map;

        /**
         * @param map Objects to put.
         */
        PutAllCommand(Map<Object, Object> map) {
            this.map = map;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.putAllAsync(map).chain(new FixedResult(true));
        }
    }

    /** */
    private static class RemoveCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected final Object key;

        /**
         * @param key Key.
         */
        RemoveCommand(Object key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.removeAsync(key);
        }
    }

    /** */
    private static class RemoveValueCommand extends GetAndPutCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param key Key.
         * @param val Value.
         */
        RemoveValueCommand(Object key, Object val) {
            super(key, val);
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.removeAsync(key, val);
        }
    }

    /** */
    private static class GetAndRemoveCommand extends RemoveCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param key Key.
         */
        GetAndRemoveCommand(Object key) {
            super(key);
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.getAndRemoveAsync(key);
        }
    }

    /** */
    private static class RemoveAllCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Collection<Object> keys;

        /**
         * @param keys Keys to remove.
         */
        RemoveAllCommand(Collection<Object> keys) {
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return (F.isEmpty(keys) ? c.removeAllAsync() : c.removeAllAsync(keys))
                .chain(new FixedResult(true));
        }
    }

    /** */
    private static class CasCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object exp;

        /** */
        private final Object val;

        /** */
        private final Object key;

        /**
         * @param exp Expected previous value.
         * @param val New value.
         * @param key Key.
         */
        CasCommand(Object exp, Object val, Object key) {
            this.val = val;
            this.exp = exp;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return exp == null && val == null ? c.removeAsync(key) :
                exp == null ? c.putIfAbsentAsync(key, val) :
                    val == null ? c.removeAsync(key, exp) :
                        c.replaceAsync(key, exp, val);
        }
    }

    /** */
    private static class PutCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final Long ttl;

        /** */
        private final Object val;

        /**
         * @param key Key.
         * @param ttl TTL.
         * @param val Value.
         */
        PutCommand(Object key, Long ttl, Object val) {
            this.key = key;
            this.ttl = ttl;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            if (ttl != null && ttl > 0) {
                Duration duration = new Duration(MILLISECONDS, ttl);

                c = c.withExpiryPolicy(new ModifiedExpiryPolicy(duration));
            }

            return c.putAsync(key, val);
        }
    }

    /** */
    private static class AddCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final Long ttl;

        /** */
        private final Object val;

        /**
         * @param key Key.
         * @param ttl TTL.
         * @param val Value.
         */
        AddCommand(Object key, Long ttl, Object val) {
            this.key = key;
            this.ttl = ttl;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            if (ttl != null && ttl > 0) {
                Duration duration = new Duration(MILLISECONDS, ttl);

                c = c.withExpiryPolicy(new ModifiedExpiryPolicy(duration));
            }

            return c.putIfAbsentAsync(key, val);
        }
    }

    /** */
    private static class ReplaceCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final Long ttl;

        /** */
        private final Object val;

        /**
         * @param key Key.
         * @param ttl TTL.
         * @param val Value.
         */
        ReplaceCommand(Object key, Long ttl, Object val) {
            this.key = key;
            this.ttl = ttl;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            if (ttl != null && ttl > 0) {
                Duration duration = new Duration(MILLISECONDS, ttl);

                c = c.withExpiryPolicy(new ModifiedExpiryPolicy(duration));
            }

            return c.replaceAsync(key, val);
        }
    }

    /** */
    private static class AppendCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final GridRestCacheRequest req;

        /**
         * @param key Key.
         * @param req Operation request.
         */
        AppendCommand(Object key, GridRestCacheRequest req) {
            this.key = key;
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx)
            throws IgniteCheckedException {
            return appendOrPrepend(ctx, c, key, req, false);
        }
    }

    /** */
    private static class PrependCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final GridRestCacheRequest req;

        /**
         * @param key Key.
         * @param req Operation request.
         */
        PrependCommand(Object key, GridRestCacheRequest req) {
            this.key = key;
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx)
            throws IgniteCheckedException {
            return appendOrPrepend(ctx, c, key, req, true);
        }
    }

    /** */
    private static class MetricsCommand extends CacheCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            CacheMetrics metrics = c.cache().localMetrics();

            assert metrics != null;

            return new GridFinishedFuture<Object>(new GridCacheRestMetrics(
                (int)metrics.getCacheGets(),
                (int)(metrics.getCacheRemovals() + metrics.getCachePuts()),
                (int)metrics.getCacheHits(),
                (int)metrics.getCacheMisses())
            );
        }
    }

    /** */
    private static class SizeCommand extends CacheCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(IgniteInternalCache<Object, Object> c, GridKernalContext ctx) {
            return c.sizeAsync(new CachePeekMode[] {CachePeekMode.PRIMARY});
        }
    }
}
