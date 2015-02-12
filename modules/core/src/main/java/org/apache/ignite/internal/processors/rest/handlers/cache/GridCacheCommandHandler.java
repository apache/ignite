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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.rest.*;
import org.apache.ignite.internal.processors.rest.handlers.*;
import org.apache.ignite.internal.processors.rest.request.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.internal.GridClosureCallMode.*;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.*;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Command handler for API requests.
 */
public class GridCacheCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(
        CACHE_GET,
        CACHE_GET_ALL,
        CACHE_PUT,
        CACHE_ADD,
        CACHE_PUT_ALL,
        CACHE_REMOVE,
        CACHE_REMOVE_ALL,
        CACHE_REPLACE,
        CACHE_CAS,
        CACHE_APPEND,
        CACHE_PREPEND,
        CACHE_METRICS
    );

    /** Requests with required parameter {@code key}. */
    private static final EnumSet<GridRestCommand> KEY_REQUIRED_REQUESTS = EnumSet.of(
        CACHE_GET,
        CACHE_PUT,
        CACHE_ADD,
        CACHE_REMOVE,
        CACHE_REPLACE,
        ATOMIC_INCREMENT,
        ATOMIC_DECREMENT,
        CACHE_CAS,
        CACHE_APPEND,
        CACHE_PREPEND
    );

    /** */
    private static final CacheFlag[] EMPTY_FLAGS = new CacheFlag[0];

    /**
     * @param ctx Context.
     */
    public GridCacheCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /**
     * Retrieves cache flags from corresponding bits.
     *
     * @param cacheFlagsBits Integer representation of cache flags bit set.
     * @return Array of cache flags.
     */
    public static CacheFlag[] parseCacheFlags(int cacheFlagsBits) {
        if (cacheFlagsBits == 0)
            return EMPTY_FLAGS;

        EnumSet<CacheFlag> flagSet = EnumSet.noneOf(CacheFlag.class);

        if ((cacheFlagsBits & 1) != 0)
            flagSet.add(CacheFlag.SKIP_STORE);

        if ((cacheFlagsBits & (1 << 1)) != 0)
            flagSet.add(CacheFlag.SKIP_SWAP);

        if ((cacheFlagsBits & (1 << 2)) != 0)
            flagSet.add(CacheFlag.SYNC_COMMIT);

        if ((cacheFlagsBits & (1 << 4)) != 0)
            flagSet.add(CacheFlag.INVALIDATE);

        return flagSet.toArray(new CacheFlag[flagSet.size()]);
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

        final CacheFlag[] flags = parseCacheFlags(req0.cacheFlags());

        try {
            GridRestCommand cmd = req0.command();

            if (key == null && KEY_REQUIRED_REQUESTS.contains(cmd))
                throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("key"));

            final Long ttl = req0.ttl();

            IgniteInternalFuture<GridRestResponse> fut;

            switch (cmd) {
                case CACHE_GET: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new GetCommand(key), req.portableMode());

                    break;
                }

                case CACHE_GET_ALL: {
                    Set<Object> keys = req0.values().keySet();

                    if (F.isEmpty(keys))
                        throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("keys"));

                    // HashSet wrapping for correct serialization
                    keys = new HashSet<>(keys);

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new GetAllCommand(keys), req.portableMode());

                    break;
                }

                case CACHE_PUT: {
                    final Object val = req0.value();

                    if (val == null)
                        throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("val"));

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key, new
                        PutCommand(key, ttl, val), req.portableMode());

                    break;
                }

                case CACHE_ADD: {
                    final Object val = req0.value();

                    if (val == null)
                        throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("val"));

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new AddCommand(key, ttl, val), req.portableMode());

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

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new PutAllCommand(map), req.portableMode());

                    break;
                }

                case CACHE_REMOVE: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new RemoveCommand(key), req.portableMode());

                    break;
                }

                case CACHE_REMOVE_ALL: {
                    Map<Object, Object> map = req0.values();

                    // HashSet wrapping for correct serialization
                    Set<Object> keys = map == null ? null : new HashSet<>(map.keySet());

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new RemoveAllCommand(keys), req.portableMode());

                    break;
                }

                case CACHE_REPLACE: {
                    final Object val = req0.value();

                    if (val == null)
                        throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("val"));

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new ReplaceCommand(key, ttl, val), req.portableMode());

                    break;
                }

                case CACHE_CAS: {
                    final Object val1 = req0.value();
                    final Object val2 = req0.value2();

                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new CasCommand(val2, val1, key), req.portableMode());

                    break;
                }

                case CACHE_APPEND: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new AppendCommand(key, req0), req.portableMode());

                    break;
                }

                case CACHE_PREPEND: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, flags, key,
                        new PrependCommand(key, req0), req.portableMode());

                    break;
                }

                case CACHE_METRICS: {
                    fut = executeCommand(req.destinationId(), req.clientId(), cacheName, key, new MetricsCommand());

                    break;
                }

                default:
                    throw new IllegalArgumentException("Invalid command for cache handler: " + req);
            }

            return fut;
        }
        catch (IgniteException e) {
            U.error(log, "Failed to execute cache command: " + req, e);

            return new GridFinishedFuture<>(ctx, e);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to execute cache command: " + req, e);

            return new GridFinishedFuture<>(ctx, e);
        }
        finally {
            if (log.isDebugEnabled())
                log.debug("Handled cache REST request: " + req);
        }
    }

    /**
     * Executes command on flagged cache projection. Checks {@code destId} to find
     * if command could be performed locally or routed to a remote node.
     *
     * @param destId Target node Id for the operation.
     *      If {@code null} - operation could be executed anywhere.
     * @param clientId Client ID.
     * @param cacheName Cache name.
     * @param flags Cache flags.
     * @param key Key to set affinity mapping in the response.
     * @param op Operation to perform.
     * @param keepPortable Keep portable flag.
     * @return Operation result in future.
     * @throws IgniteCheckedException If failed
     */
    private IgniteInternalFuture<GridRestResponse> executeCommand(
        @Nullable UUID destId,
        UUID clientId,
        final String cacheName,
        final CacheFlag[] flags,
        final Object key,
        final CacheProjectionCommand op,
        final boolean keepPortable) throws IgniteCheckedException {

        final boolean locExec =
            destId == null || destId.equals(ctx.localNodeId()) || replicatedCacheAvailable(cacheName);

        if (locExec) {
            CacheProjection<?,?> prj = localCache(cacheName).forSubjectId(clientId).flagsOn(flags);

            if (keepPortable)
                prj = prj.keepPortable();

            return op.apply((CacheProjection<Object, Object>)prj, ctx).
                chain(resultWrapper((CacheProjection<Object, Object>)prj, key));
        }
        else {
            ClusterGroup prj = ctx.grid().forPredicate(F.nodeForNodeId(destId));

            ctx.task().setThreadContext(TC_NO_FAILOVER, true);

            return ctx.closure().callAsync(BALANCE,
                new FlaggedCacheOperationCallable(clientId, cacheName, flags, op, key, keepPortable),
                prj.nodes());
        }
    }

    /**
     * Executes command on cache. Checks {@code destId} to find
     * if command could be performed locally or routed to a remote node.
     *
     * @param destId Target node Id for the operation.
     *      If {@code null} - operation could be executed anywhere.
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
            final CacheProjection<Object, Object> cache = localCache(cacheName).forSubjectId(clientId);

            return op.apply(cache, ctx).chain(resultWrapper(cache, key));
        }
        else {
            ClusterGroup prj = ctx.grid().forPredicate(F.nodeForNodeId(destId));

            ctx.task().setThreadContext(TC_NO_FAILOVER, true);

            return ctx.closure().callAsync(BALANCE,
                new CacheOperationCallable(clientId, cacheName, op, key),
                prj.nodes());
        }
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
        final CacheProjection<Object, Object> cache,
        final Object key, GridRestCacheRequest req, final boolean prepend) throws IgniteCheckedException {
        assert cache != null;
        assert key != null;
        assert req != null;

        final Object val = req.value();

        if (val == null)
            throw new IgniteCheckedException(GridRestCommandHandlerAdapter.missingParameter("val"));

        return ctx.closure().callLocalSafe(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (IgniteInternalTx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                    Object curVal = cache.get(key);

                    if (curVal == null)
                        return false;

                    // Modify current value with appendix one.
                    Object newVal = appendOrPrepend(curVal, val, !prepend);

                    // Put new value asynchronously.
                    cache.putx(key, newVal);

                    tx.commit();
                }

                return true;
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
    private static Object appendOrPrepend(Object origVal, Object appendVal, boolean appendPlc) throws IgniteCheckedException {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheCommandHandler.class, this);
    }

    /**
     * Creates a transformation function from {@link CacheCommand}'s results into {@link GridRestResponse}.
     *
     * @param c Cache instance to obtain affinity data.
     * @param key Affinity key for previous operation.
     * @return Rest response.
     */
    private static IgniteClosure<IgniteInternalFuture<?>, GridRestResponse> resultWrapper(
        final CacheProjection<Object, Object> c, @Nullable final Object key) {
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
     * @param cacheName Cache name.
     * @return If replicated cache with given name is locally available.
     */
    private boolean replicatedCacheAvailable(String cacheName) {
        GridCacheAdapter<Object,Object> cache = ctx.cache().internalCache(cacheName);

        return cache != null && cache.configuration().getCacheMode() == CacheMode.REPLICATED;
    }

    /**
     * Used for test purposes.
     *
     * @param cacheName Name of the cache.
     * @return Instance on the named cache.
     * @throws IgniteCheckedException If cache not found.
     */
    protected GridCacheProjectionEx<Object, Object> localCache(String cacheName) throws IgniteCheckedException {
        GridCacheProjectionEx<Object, Object> cache = (GridCacheProjectionEx<Object, Object>)ctx.cache().cache(cacheName);

        if (cache == null)
            throw new IgniteCheckedException(
                "Failed to find cache for given cache name (null for default cache): " + cacheName);

        return cache;
    }

    /**
     * @param ignite Grid instance.
     * @param cacheName Name of the cache.
     * @return Instance on the named cache.
     * @throws IgniteCheckedException If cache not found.
     */
    private static GridCacheProjectionEx<Object, Object> cache(Ignite ignite, String cacheName) throws IgniteCheckedException {
        GridCache<Object, Object> cache = ignite.cache(cacheName);

        if (cache == null)
            throw new IgniteCheckedException(
                "Failed to find cache for given cache name (null for default cache): " + cacheName);

        return (GridCacheProjectionEx<Object, Object>)cache;
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
        extends IgniteClosure2X<CacheProjection<Object, Object>, GridKernalContext, IgniteInternalFuture<?>> {
        /** */
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     * Type alias.
     */
    private abstract static class CacheProjectionCommand
        extends IgniteClosure2X<CacheProjection<Object, Object>, GridKernalContext, IgniteInternalFuture<?>> {
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

        /** Client ID. */
        private UUID clientId;

        /** */
        private final String cacheName;

        /** */
        private final CacheFlag[] flags;

        /** */
        private final CacheProjectionCommand op;

        /** */
        private final Object key;

        /** */
        private final boolean keepPortable;

        /** */
        @IgniteInstanceResource
        private Ignite g;

        /**
         * @param clientId Client ID.
         * @param cacheName Cache name.
         * @param flags Flags.
         * @param op Operation.
         * @param key Key.
         * @param keepPortable Keep portable flag.
         */
        private FlaggedCacheOperationCallable(UUID clientId, String cacheName, CacheFlag[] flags,
            CacheProjectionCommand op, Object key, boolean keepPortable) {
            this.clientId = clientId;
            this.cacheName = cacheName;
            this.flags = flags;
            this.op = op;
            this.key = key;
            this.keepPortable = keepPortable;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            CacheProjection<?, ?> prj = cache(g, cacheName).forSubjectId(clientId).flagsOn(flags);

            if (keepPortable)
                prj = prj.keepPortable();

            // Need to apply both operation and response transformation remotely
            // as cache could be inaccessible on local node and
            // exception processing should be consistent with local execution.
            return op.apply((CacheProjection<Object, Object>)prj, ((IgniteKernal)g).context()).
                chain(resultWrapper((CacheProjection<Object, Object>)prj, key)).get();
        }
    }

    /**
     * Class for cache operations.
     */
    @GridInternal
    private static class CacheOperationCallable implements Callable<GridRestResponse>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Client ID. */
        private UUID clientId;

        /** */
        private final String cacheName;

        /** */
        private final CacheCommand op;

        /** */
        private final Object key;

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
            final CacheProjection<Object, Object> cache = cache(g, cacheName).forSubjectId(clientId);

            // Need to apply both operation and response transformation remotely
            // as cache could be inaccessible on local node and
            // exception processing should be consistent with local execution.
            return op.apply(cache, ((IgniteKernal)g).context()).chain(resultWrapper(cache, key)).get();
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
            return c.getAsync(key);
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
            return c.putAllAsync(map).chain(new FixedResult(true));
        }
    }

    /** */
    private static class RemoveCommand extends CacheProjectionCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /**
         * @param key Key.
         */
        RemoveCommand(Object key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
            return c.removexAsync(key);
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
            return exp == null && val == null ? c.removexAsync(key) :
                exp == null ? c.putxIfAbsentAsync(key, val) :
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
            if (ttl != null && ttl > 0) {
                Duration duration = new Duration(MILLISECONDS, ttl);

                c = ((GridCacheProjectionEx<Object, Object>)c).withExpiryPolicy(new ModifiedExpiryPolicy(duration));
            }

            return c.putxAsync(key, val);
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
            if (ttl != null && ttl > 0) {
                Duration duration = new Duration(MILLISECONDS, ttl);

                c = ((GridCacheProjectionEx<Object, Object>)c).withExpiryPolicy(new ModifiedExpiryPolicy(duration));
            }

            return c.putxIfAbsentAsync(key, val);
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
            if (ttl != null && ttl > 0) {
                Duration duration = new Duration(MILLISECONDS, ttl);

                c = ((GridCacheProjectionEx<Object, Object>)c).withExpiryPolicy(new ModifiedExpiryPolicy(duration));
            }

            return c.replacexAsync(key, val);
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx)
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
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx)
            throws IgniteCheckedException {
            return appendOrPrepend(ctx, c, key, req, true);
        }
    }

    /** */
    private static class MetricsCommand extends CacheCommand {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> applyx(CacheProjection<Object, Object> c, GridKernalContext ctx) {
            CacheMetrics metrics = c.cache().metrics();

            assert metrics != null;

            return new GridFinishedFuture<Object>(ctx, new GridCacheRestMetrics(
                (int)metrics.getCacheGets(),
                (int)(metrics.getCacheRemovals() + metrics.getCachePuts()),
                (int)metrics.getCacheHits(),
                (int)metrics.getCacheMisses())
            );
        }
    }
}
