/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.license.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.product.GridProductEdition.*;
import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;

/**
 * Command handler for API requests.
 */
public class GridCacheCommandHandler extends GridRestCommandHandlerAdapter {
    /** */
    private static final GridCacheFlag[] EMPTY_FLAGS = new GridCacheFlag[0];

    /**
     * @param ctx Context.
     */
    public GridCacheCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean supported(GridRestCommand cmd) {
        switch (cmd) {
            case CACHE_GET:
            case CACHE_GET_ALL:
            case CACHE_PUT:
            case CACHE_ADD:
            case CACHE_PUT_ALL:
            case CACHE_REMOVE:
            case CACHE_REMOVE_ALL:
            case CACHE_REPLACE:
            case CACHE_INCREMENT:
            case CACHE_DECREMENT:
            case CACHE_CAS:
            case CACHE_APPEND:
            case CACHE_PREPEND:
            case CACHE_METRICS:
                return true;

            default:
                return false;
        }
    }

    /**
     * Retrieves cache flags from corresponding bits.
     *
     * @param cacheFlagsStr String representation of cache flags bit set.
     * @return Array of cache flags.
     */
    public static GridCacheFlag[] parseCacheFlags(String cacheFlagsStr) {
        EnumSet<GridCacheFlag> flagSet = EnumSet.noneOf(GridCacheFlag.class);

        int cacheFlagsBits = Integer.parseInt(cacheFlagsStr);

        if ((cacheFlagsBits & 1) != 0)
            flagSet.add(GridCacheFlag.SKIP_STORE);

        if ((cacheFlagsBits & (1 << 1)) != 0)
            flagSet.add(GridCacheFlag.SKIP_SWAP);

        if ((cacheFlagsBits & (1 << 2)) != 0)
            flagSet.add(GridCacheFlag.SYNC_COMMIT);

        if ((cacheFlagsBits & (1 << 4)) != 0)
            flagSet.add(GridCacheFlag.INVALIDATE);

        return flagSet.toArray(new GridCacheFlag[flagSet.size()]);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridRestResponse> handleAsync(final GridRestRequest req) {
        assert req != null;

        GridLicenseUseRegistry.onUsage(DATA_GRID, getClass());

        if (log.isDebugEnabled())
            log.debug("Handling cache REST request: " + req);

        final String cacheName = value("cacheName", req);

        String cacheFlagsOn = value("cacheFlags", req);
        final GridCacheFlag[] flags = F.isEmpty(cacheFlagsOn) ? EMPTY_FLAGS : parseCacheFlags(cacheFlagsOn);

        final Object key = value("key", req);
        final Collection<Object> keys = values("k", req);

        try {
            GridRestCommand cmd = req.getCommand();

            if (key == null && (cmd == CACHE_GET || cmd == CACHE_PUT || cmd == CACHE_ADD || cmd == CACHE_REMOVE ||
                cmd == CACHE_REPLACE || cmd == CACHE_INCREMENT || cmd == CACHE_DECREMENT || cmd == CACHE_CAS ||
                cmd == CACHE_APPEND || cmd == CACHE_PREPEND))
                throw new GridException(missingParameter("key"));

            if (F.isEmpty(keys) && (cmd == CACHE_GET_ALL || cmd == CACHE_PUT_ALL))
                throw new GridException(missingParameter("k1"));

            Object exp = value("exp", req);
            final Long ttl = exp instanceof String ? Long.valueOf((String)exp) : (Long)exp;

            GridFuture<GridRestResponse> fut;

            switch (cmd) {
                case CACHE_GET:
                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new GetCommand(key)); break;

                case CACHE_GET_ALL:
                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new GetAllCommand(keys)); break;

                case CACHE_PUT: {
                    final Object val = value("val", req);

                    if (val == null)
                        throw new GridException(missingParameter("val"));

                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new PutCommand(key, ttl, val));
                } break;

                case CACHE_ADD: {
                    final Object val = value("val", req);

                    if (val == null)
                        throw new GridException(missingParameter("val"));

                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new AddCommand(key, ttl, val));
                } break;

                case CACHE_PUT_ALL: {
                    Collection<Object> vals = values("v", req);

                    if (vals == null)
                        throw new GridException(missingParameter("v"));

                    if (keys.size() != vals.size())
                        throw new GridException("Number of keys and values must be equal.");

                    final Map<Object, Object> map = new GridLeanMap<>(keys.size());

                    Iterator keysIt = keys.iterator();
                    Iterator valsIt = vals.iterator();

                    while (keysIt.hasNext() && valsIt.hasNext()) {
                        Object nextKey = keysIt.next();
                        Object nextVal = valsIt.next();

                        if (nextKey == null)
                            throw new GridException("Failing putAll operation (null keys are not allowed).");

                        if (nextVal == null)
                            throw new GridException("Failing putAll operation (null values are not allowed).");

                        map.put(nextKey, nextVal);
                    }

                    assert !keysIt.hasNext() && !valsIt.hasNext();

                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new PutAllCommand(map));
                } break;

                case CACHE_REMOVE:
                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new RemoveCommand(key)); break;

                case CACHE_REMOVE_ALL:
                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new RemoveAllCommand(keys)); break;

                case CACHE_REPLACE: {
                    final Object val = value("val", req);

                    if (val == null)
                        throw new GridException(missingParameter("val"));

                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new ReplaceCommand(key, ttl, val));
                } break;

                case CACHE_INCREMENT:
                    fut = executeCommand(req.getDestId(), cacheName, key, new IncrementCommand(key, req)); break;

                case CACHE_DECREMENT:
                    fut = executeCommand(req.getDestId(), cacheName, key, new DecrementCommand(key, req)); break;

                case CACHE_CAS:
                    final Object val1 = value("val1", req);
                    final Object val2 = value("val2", req);

                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new CasCommand(val2, val1, key));
                    break;

                case CACHE_APPEND:
                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new AppendCommand(key, req)); break;

                case CACHE_PREPEND:
                    fut = executeCommand(req.getDestId(), cacheName, flags, key, new PrependCommand(key, req)); break;

                case CACHE_METRICS:
                    fut = executeCommand(req.getDestId(), cacheName, key, new MetricsCommand()); break;

                default:
                    throw new IllegalArgumentException("Invalid command for cache handler: " + req);
            }

            return fut;
        }
        catch (GridRuntimeException e) {
            U.error(log, "Failed to execute cache command: " + req, e);

            return new GridFinishedFuture<>(ctx, e);
        }
        catch (GridException e) {
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
     *  If {@code null} - operation could be executed anywhere.
     * @param cacheName Cache name.
     * @param flags Cache flags.
     * @param key Key to set affinity mapping in the response.
     * @param op Operation to perform.
     * @return Operation result in future.
     * @throws GridException If failed
     */
    private GridFuture<GridRestResponse> executeCommand(
        @Nullable UUID destId,
        final String cacheName,
        final GridCacheFlag[] flags,
        final Object key,
        final CacheProjectionCommand op) throws GridException {
        final boolean locExec =
            destId == null || destId.equals(ctx.localNodeId()) || replicatedCacheAvailable(cacheName);

        if (locExec) {
            final GridCacheProjection<Object, Object> prj = localCache(cacheName).flagsOn(flags);

            return op.apply(prj, ctx).chain(resultWrapper(prj, key));
        }
        else {
            return ctx.grid().forPredicate(F.nodeForNodeId(destId)).compute().withNoFailover().
                call(new FlaggedCacheOperationCallable(cacheName, flags, op, key));
        }
    }

    /**
     * Executes command on cache. Checks {@code destId} to find
     * if command could be performed locally or routed to a remote node.
     *
     * @param destId Target node Id for the operation.
     *  If {@code null} - operation could be executed anywhere.
     * @param cacheName Cache name.
     * @param key Key to set affinity mapping in the response.
     * @param op Operation to perform.
     * @return Operation result in future.
     * @throws GridException If failed
     */
    private GridFuture<GridRestResponse> executeCommand(
        @Nullable UUID destId,
        final String cacheName,
        final Object key,
        final CacheCommand op) throws GridException {
        final boolean locExec = destId == null || destId.equals(ctx.localNodeId()) ||
            ctx.cache().cache(cacheName) != null;

        if (locExec) {
            final GridCache<Object, Object> cache = localCache(cacheName);

            return op.apply(cache, ctx).chain(resultWrapper(cache, key));
        }
        else {
            return ctx.grid().forPredicate(F.nodeForNodeId(destId)).compute().withNoFailover().
                call(new CacheOperationCallable(cacheName, op, key));
        }
    }

    /**
     * Handles increment and decrement commands.
     *
     * @param cache Cache.
     * @param key Key.
     * @param req Request.
     * @param decr Whether to decrement (increment otherwise).
     * @return Future of operation result.
     * @throws GridException In case of error.
     */
    private static GridFuture<?> incrementOrDecrement(GridCache<Object, Object> cache, String key,
        GridRestRequest req, final boolean decr) throws GridException {
        assert cache != null;
        assert key != null;
        assert req != null;

        Object initObj = value("init", req);
        Object deltaObj = value("delta", req);

        Long init = null;

        if (initObj != null) {
            if (initObj instanceof String) {
                try {
                    init = Long.valueOf((String)initObj);
                }
                catch (NumberFormatException ignored) {
                    throw new GridException(invalidNumericParameter("init"));
                }
            }
            else if (initObj instanceof Long)
                init = (Long)initObj;
        }

        if (deltaObj == null)
            throw new GridException(missingParameter("delta"));

        Long delta = null;

        if (deltaObj instanceof String) {
            try {
                delta = Long.valueOf((String)deltaObj);
            }
            catch (NumberFormatException ignored) {
                throw new GridException(invalidNumericParameter("delta"));
            }
        }
        else if (deltaObj instanceof Long)
            delta = (Long)deltaObj;

        final GridCacheAtomicLong l = cache.dataStructures().atomicLong(key, init != null ? init : 0, true);

        final Long d = delta;

        return ((GridKernal)cache.gridProjection().grid()).context().closure().callLocalSafe(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return l.addAndGet(decr ? -d : d);
            }
        }, false);
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
     * @throws GridException In case of any exception.
     */
    private static GridFuture<?> appendOrPrepend(
        final GridKernalContext ctx,
        final GridCacheProjection<Object, Object> cache,
        final Object key, GridRestRequest req, final boolean prepend) throws GridException {
        assert cache != null;
        assert key != null;
        assert req != null;

        final Object val = value("val", req);

        if (val == null)
            throw new GridException(missingParameter("val"));

        return ctx.closure().callLocalSafe(new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ);

                try {
                    Object curVal = cache.get(key);

                    if (curVal == null)
                        return false;

                    // Modify current value with appendix one.
                    Object newVal = appendOrPrepend(curVal, val, !prepend);

                    // Put new value asynchronously.
                    cache.putx(key, newVal);

                    tx.commit();
                }
                finally {
                    tx.close();
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
     * @throws GridException In case of grid exceptions.
     */
    private static Object appendOrPrepend(Object origVal, Object appendVal, boolean appendPlc) throws GridException {
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

        throw new GridException("Incompatible types [appendVal=" + appendVal + ", old=" + origVal + ']');
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
    private static GridClosureX<GridFuture<?>, GridRestResponse> resultWrapper(
        final GridCacheProjection<Object, Object> c, @Nullable final Object key) {
        return new CX1<GridFuture<?>, GridRestResponse>() {
            @Override public GridRestResponse applyx(GridFuture<?> f) throws GridException {
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

        return cache != null && cache.configuration().getCacheMode() == GridCacheMode.REPLICATED;
    }

    /**
     * Used for test purposes.
     *
     * @param cacheName Name of the cache.
     * @return Instance on the named cache.
     * @throws GridException If cache not found.
     */
    protected GridCache<Object, Object> localCache(String cacheName) throws GridException {
        GridCache<Object, Object> cache = ctx.cache().cache(cacheName);

        if (cache == null)
            throw new GridException(
                "Failed to find cache for given cache name (null for default cache): " + cacheName);

        return cache;
    }

    /**
     * @param grid Grid instance.
     * @param cacheName Name of the cache.
     * @return Instance on the named cache.
     * @throws GridException If cache not found.
     */
    private static GridCache<Object, Object> cache(Grid grid, String cacheName) throws GridException {
        GridCache<Object, Object> cache = grid.cache(cacheName);

        if (cache == null)
            throw new GridException(
                "Failed to find cache for given cache name (null for default cache): " + cacheName);

        return cache;
    }

    /**
     * Fixed result closure.
     */
    private static final class FixedResult extends CX1<GridFuture<?>, Object> {
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
        @Override public Object applyx(GridFuture<?> f) throws GridException {
            f.get();

            return res;
        }
    }

    /**
     * Type alias.
     */
    private abstract static class CacheCommand
        extends GridClosure2X<GridCache<Object, Object>, GridKernalContext, GridFuture<?>> {
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     * Type alias.
     */
    private abstract static class CacheProjectionCommand
        extends GridClosure2X<GridCacheProjection<Object, Object>, GridKernalContext, GridFuture<?>> {
        private static final long serialVersionUID = 0L;

        // No-op.
    }

    /**
     * Class for flagged cache operations.
     */
    @GridInternal
    private static class FlaggedCacheOperationCallable implements Callable<GridRestResponse>, Serializable {
        private static final long serialVersionUID = 0L;

        /** */
        private final String cacheName;

        /** */
        private final GridCacheFlag[] flags;

        /** */
        private final CacheProjectionCommand op;

        /** */
        private final Object key;

        /** */
        @GridInstanceResource
        private Grid g;

        /**
         * @param cacheName Cache name.
         * @param flags Flags.
         * @param op Operation.
         * @param key Key.
         */
        private FlaggedCacheOperationCallable(String cacheName, GridCacheFlag[] flags, CacheProjectionCommand op,
            Object key) {
            this.cacheName = cacheName;
            this.flags = flags;
            this.op = op;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            final GridCacheProjection<Object, Object> prj = cache(g, cacheName).flagsOn(flags);

            // Need to apply both operation and response transformation remotely
            // as cache could be inaccessible on local node and
            // exception processing should be consistent with local execution.
            return op.apply(prj, ((GridKernal)g).context()).chain(resultWrapper(prj, key)).get();
        }
    }

    /**
     * Class for cache operations.
     */
    @GridInternal
    private static class CacheOperationCallable implements Callable<GridRestResponse>, Serializable {
        private static final long serialVersionUID = 0L;

        /** */
        private final String cacheName;

        /** */
        private final CacheCommand op;

        /** */
        private final Object key;

        /** */
        @GridInstanceResource
        private Grid g;

        /**
         * @param cacheName Cache name.
         * @param op Operation.
         * @param key Key.
         */
        private CacheOperationCallable(String cacheName, CacheCommand op, Object key) {
            this.cacheName = cacheName;
            this.op = op;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            final GridCache<Object, Object> cache = cache(g, cacheName);

            // Need to apply both operation and response transformation remotely
            // as cache could be inaccessible on local node and
            // exception processing should be consistent with local execution.
            return op.apply(cache, ((GridKernal)g).context()).chain(resultWrapper(cache, key)).get();
        }
    }

    /** */
    private static class GetCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            return c.getAsync(key);
        }
    }

    /** */
    private static class GetAllCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            return c.getAllAsync(keys);
        }
    }

    /** */
    private static class PutAllCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            return c.putAllAsync(map).chain(new FixedResult(true));
        }
    }

    /** */
    private static class RemoveCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            return c.removexAsync(key);
        }
    }

    /** */
    private static class RemoveAllCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            return (F.isEmpty(keys) ? c.removeAllAsync() : c.removeAllAsync(keys))
                .chain(new FixedResult(true));
        }
    }

    /** */
    private static class CasCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            return exp == null && val == null ? c.removexAsync(key) :
                exp == null ? c.putxIfAbsentAsync(key, val) :
                    val == null ? c.removeAsync(key, exp) :
                        c.replaceAsync(key, exp, val);
        }
    }

    /** */
    private static class PutCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            GridCacheEntry<Object, Object> entry = c.entry(key);

            if (entry != null) {
                if (ttl != null)
                    entry.timeToLive(ttl);

                return entry.setxAsync(val);
            }
            else
                return new GridFinishedFuture<Object>(ctx, false);
        }
    }

    /** */
    private static class AddCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            GridCacheEntry<Object, Object> entry = c.entry(key);

            if (entry != null) {
                if (ttl != null)
                    entry.timeToLive(ttl);

                return entry.setxIfAbsentAsync(val);
            }
            else
                return new GridFinishedFuture<Object>(ctx, false);
        }
    }

    /** */
    private static class ReplaceCommand extends CacheProjectionCommand {
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
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx) {
            GridCacheEntry<Object, Object> entry = c.entry(key);

            if (entry != null) {
                if (ttl != null)
                    entry.timeToLive(ttl);

                return entry.replacexAsync(val);
            }
            else
                return new GridFinishedFuture<Object>(ctx, false);
        }
    }

    /** */
    private static class IncrementCommand extends CacheCommand {
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final GridRestRequest req;

        /**
         * @param key Key.
         * @param req Operation request.
         */
        IncrementCommand(Object key, GridRestRequest req) {
            this.key = key;
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public GridFuture<?> applyx(GridCache<Object, Object> c, GridKernalContext ctx)
            throws GridException {
            return incrementOrDecrement(c, (String)key, req, false);
        }
    }

    /** */
    private static class DecrementCommand extends CacheCommand {
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final GridRestRequest req;

        /**
         * @param key Key.
         * @param req Operation request.
         */
        DecrementCommand(Object key, GridRestRequest req) {
            this.key = key;
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public GridFuture<?> applyx(GridCache<Object, Object> c, GridKernalContext ctx) throws GridException {
            return incrementOrDecrement(c, (String)key, req, true);
        }
    }

    /** */
    private static class AppendCommand extends CacheProjectionCommand {
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final GridRestRequest req;

        /**
         * @param key Key.
         * @param req Operation request.
         */
        AppendCommand(Object key, GridRestRequest req) {
            this.key = key;
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx)
            throws GridException {
            return appendOrPrepend(ctx, c, key, req, false);
        }
    }

    /** */
    private static class PrependCommand extends CacheProjectionCommand {
        private static final long serialVersionUID = 0L;

        /** */
        private final Object key;

        /** */
        private final GridRestRequest req;

        /**
         * @param key Key.
         * @param req Operation request.
         */
        PrependCommand(Object key, GridRestRequest req) {
            this.key = key;
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public GridFuture<?> applyx(GridCacheProjection<Object, Object> c, GridKernalContext ctx)
            throws GridException {
            return appendOrPrepend(ctx, c, key, req, true);
        }
    }

    /** */
    private static class MetricsCommand extends CacheCommand {
        private static final long serialVersionUID = 0L;


        /** {@inheritDoc} */
        @Override public GridFuture<?> applyx(GridCache<Object, Object> c, GridKernalContext ctx) {
            GridCacheMetrics metrics = c.metrics();

            assert metrics != null;

            return new GridFinishedFuture<Object>(ctx, new GridCacheRestMetrics(
                metrics.createTime(), metrics.readTime(), metrics.writeTime(),
                metrics.reads(), metrics.writes(), metrics.hits(), metrics.misses()));
        }
    }
}
