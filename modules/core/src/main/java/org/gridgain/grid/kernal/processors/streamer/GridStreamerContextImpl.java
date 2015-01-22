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

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.streamer.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.streamer.task.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
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
    @Override public <E> StreamerWindow<E> window() {
        return streamer.window();
    }

    /** {@inheritDoc} */
    @Override public <E> StreamerWindow<E> window(String winName) {
        StreamerWindow<E> window = streamer.window(winName);

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
    @Override public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo) throws IgniteCheckedException {
        return query(clo, Collections.<ClusterNode>emptyList());
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo, Collection<ClusterNode> nodes)
        throws IgniteCheckedException {
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
    @Override public void broadcast(IgniteInClosure<StreamerContext> clo) throws IgniteCheckedException {
        broadcast(clo, Collections.<ClusterNode>emptyList());
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteInClosure<StreamerContext> clo, Collection<ClusterNode> nodes)
        throws IgniteCheckedException {
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
        throws IgniteCheckedException {
        return reduce(clo, rdc, Collections.<ClusterNode>emptyList());
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc,
        Collection<ClusterNode> nodes) throws IgniteCheckedException {
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
