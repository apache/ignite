/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.streamer.task.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Streamer context implementation.
 */
public class GridStreamerContextImpl implements StreamerContext {
    /** Kernal context. */
    private GridKernalContext ctx;

    /** Local space. */
    private final ConcurrentMap<Object, Object> locSpace = new ConcurrentHashMap8<>();

    /** Streamer projection. */
    private AtomicReference<ClusterGroup> streamPrj = new AtomicReference<>();

    /** Streamer. */
    private IgniteStreamerEx streamer;

    /** Next stage name. */
    private String nextStageName;

    /**
     * @param ctx Kernal context.
     * @param cfg Streamer configuration.
     * @param streamer Streamer impl.
     */
    public GridStreamerContextImpl(GridKernalContext ctx, StreamerConfiguration cfg, IgniteStreamerEx streamer) {
        assert ctx != null;
        assert cfg != null;
        assert streamer != null;

        this.ctx = ctx;
        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup projection() {
        ctx.gateway().readLock();

        try {
            return projection0();
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> ConcurrentMap<K, V> localSpace() {
        return (ConcurrentMap<K, V>)locSpace;
    }

    /** {@inheritDoc} */
    @Override public <E> GridStreamerWindow<E> window() {
        return streamer.window();
    }

    /** {@inheritDoc} */
    @Override public <E> GridStreamerWindow<E> window(String winName) {
        GridStreamerWindow<E> window = streamer.window(winName);

        if (window == null)
            throw new IllegalArgumentException("Streamer window is not configured: " + winName);

        return window;
    }

    /** {@inheritDoc} */
    @Override public String nextStageName() {
        return nextStageName;
    }

    /**
     * Sets next stage name for main context.
     *
     * @param nextStageName Next stage name.
     */
    public void nextStageName(String nextStageName) {
        this.nextStageName = nextStageName;
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo) throws GridException {
        return query(clo, Collections.<ClusterNode>emptyList());
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo, Collection<ClusterNode> nodes)
        throws GridException {
        ctx.gateway().readLock();

        try {
            ClusterGroup prj = projection0();

            if (!F.isEmpty(nodes))
                prj = prj.forNodes(nodes);

            long startTime = U.currentTimeMillis();

            Collection<R> res = ctx.grid().compute(prj).execute(new GridStreamerQueryTask<>(clo, streamer.name()), null);

            streamer.onQueryCompleted(U.currentTimeMillis() - startTime, prj.nodes().size());

            return res;
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteInClosure<StreamerContext> clo) throws GridException {
        broadcast(clo, Collections.<ClusterNode>emptyList());
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteInClosure<StreamerContext> clo, Collection<ClusterNode> nodes)
        throws GridException {
        ctx.gateway().readLock();

        try {
            ClusterGroup prj = projection0();

            if (!F.isEmpty(nodes))
                prj = prj.forNodes(nodes);

            ctx.grid().compute(prj).execute(new GridStreamerBroadcastTask(clo, streamer.name()), null);
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc)
        throws GridException {
        return reduce(clo, rdc, Collections.<ClusterNode>emptyList());
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc,
        Collection<ClusterNode> nodes) throws GridException {
        ctx.gateway().readLock();

        try {
            ClusterGroup prj = projection0();

            if (!F.isEmpty(nodes))
                prj = prj.forNodes(nodes);

            return ctx.grid().compute(prj).execute(new GridStreamerReduceTask<>(clo, rdc, streamer.name()), null);
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /**
     * @return Streamer projection without grabbing read lock.
     */
    private ClusterGroup projection0() {
        ClusterGroup prj = streamPrj.get();

        if (prj == null) {
            prj = ctx.grid().forStreamer(streamer.name());

            streamPrj.compareAndSet(null, prj);

            prj = streamPrj.get();
        }

        return prj;
    }
}
