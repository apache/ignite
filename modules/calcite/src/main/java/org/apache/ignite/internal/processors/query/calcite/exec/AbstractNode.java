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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Collections;
import java.util.List;

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-12449
 */
public abstract class AbstractNode<T> implements Node<T> {
    /** */
    protected final Sink<T> target;

    /** */
    protected List<Source> sources;

    /**
     * @param target Target.
     */
    protected AbstractNode(Sink<T> target) {
        this.target = target;
    }

    /** {@inheritDoc} */
    @Override public void sources(List<Source> sources) {
        this.sources = Collections.unmodifiableList(sources);
    }

    /**
     * @param idx Index of a source to signal/
     */
    public void signal(int idx) {
        sources.get(idx).signal();
    }

    /** {@inheritDoc} */
    @Override public void signal() {
        sources.forEach(Source::signal);
    }
}
