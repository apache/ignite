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

package org.apache.ignite.internal.processors.streamer;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.streamer.*;
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
    @Override public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo) {
        return delegate.query(clo);
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> query(IgniteClosure<StreamerContext, R> clo, Collection<ClusterNode> nodes) {
        return delegate.query(clo, nodes);
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteInClosure<StreamerContext> clo) {
        delegate.broadcast(clo);
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteInClosure<StreamerContext> clo, Collection<ClusterNode> nodes) {
        delegate.broadcast(clo, nodes);
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc) {
        return delegate.reduce(clo, rdc);
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 reduce(IgniteClosure<StreamerContext, R1> clo, IgniteReducer<R1, R2> rdc,
        Collection<ClusterNode> nodes) {
        return delegate.reduce(clo, rdc, nodes);
    }
}
