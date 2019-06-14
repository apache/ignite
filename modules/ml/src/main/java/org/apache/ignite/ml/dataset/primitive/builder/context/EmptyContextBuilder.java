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

package org.apache.ignite.ml.dataset.primitive.builder.context;

import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;

/**
 * A partition {@code context} builder that makes {@link EmptyContext}.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class EmptyContextBuilder<K, V> implements PartitionContextBuilder<K, V, EmptyContext> {
    /** */
    private static final long serialVersionUID = 6620781747993467186L;

    /** {@inheritDoc} */
    @Override public EmptyContext build(LearningEnvironment env, Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize) {
        return new EmptyContext();
    }
}
