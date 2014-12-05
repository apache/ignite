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
import org.apache.ignite.streamer.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Context delegate allowing to override next stage name.
 */
public class GridStreamerContextDelegate implements StreamerContext {
    /** Context delegate. */
    private StreamerContext delegate;

    /** Next stage name. */
    private String nextStageName;

    /**
     * @param delegate Delegate object.
     * @param nextStageName Next stage name.
     */
    public GridStreamerContextDelegate(StreamerContext delegate, @Nullable String nextStageName) {
        this.delegate = delegate;
        this.nextStageName = nextStageName;
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup projection() {
        return delegate.projection();
    }

    /** {@inheritDoc} */
    @Override public <K, V> ConcurrentMap<K, V> localSpace() {
        return delegate.localSpace();
    }

    /** {@inheritDoc} */
    @Override public <E> StreamerWindow<E> window() {
        return delegate.window();
    }

    /** {@inheritDoc} */
    @Override public <E> StreamerWindow<E> window(String winName) {
        return delegate.window(winName);
    }

    /** {@inheritDoc} */
    @Override public String nextStageName() {
        return nextStageName;
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo) throws GridException {
        return delegate.query(clo);
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo, Collection<ClusterNode> nodes)
        throws GridException {
        return delegate.query(clo, nodes);
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteInClosure<StreamerContext> clo) throws GridException {
        delegate.broadcast(clo);
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteInClosure<StreamerContext> clo, Collection<ClusterNode> nodes)
        throws GridException {
        delegate.broadcast(clo, nodes);
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc)
        throws GridException {
        return delegate.reduce(clo, rdc);
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc,
        Collection<ClusterNode> nodes) throws GridException {
        return delegate.reduce(clo, rdc, nodes);
    }
}
