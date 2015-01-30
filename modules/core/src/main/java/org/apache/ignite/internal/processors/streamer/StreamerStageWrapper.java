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
import org.apache.ignite.streamer.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Stage wrapper that handles metrics calculation and time measurement.
 */
public class StreamerStageWrapper implements StreamerStage<Object> {
    /** Stage delegate. */
    private StreamerStage<Object> delegate;

    /** Stage index. */
    private int idx;

    /** Next stage name. Set after creation. */
    private String nextStageName;

    /**
     * @param delegate Delegate stage.
     * @param idx Index.
     */
    public StreamerStageWrapper(StreamerStage<Object> delegate, int idx) {
        this.delegate = delegate;
        this.idx = idx;
    }

    /**
     * @return Stage index.
     */
    public int index() {
        return idx;
    }

    /**
     * @return Next stage name in pipeline or {@code null} if this is the last stage.
     */
    @Nullable public String nextStageName() {
        return nextStageName;
    }

    /**
     * @param nextStageName Next stage name in pipeline.
     */
    public void nextStageName(String nextStageName) {
        this.nextStageName = nextStageName;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<?>> run(StreamerContext ctx, Collection<Object> evts) {
        return delegate.run(ctx, evts);
    }

    /**
     * @return Delegate.
     */
    public StreamerStage unwrap() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerStageWrapper.class, this);
    }
}
