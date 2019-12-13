/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public abstract class AbstractNode<T> implements Node<T> {
    protected final Sink<T> target;
    protected List<Source> sources;

    protected AbstractNode(Sink<T> target) {
        this.target = target;
    }

    @Override public void sources(List<Source> sources) {
        this.sources = Collections.unmodifiableList(sources);
    }

    public void signal(int idx) {
        sources.get(idx).signal();
    }

    @Override public void signal() {
        sources.forEach(Source::signal);
    }
}
