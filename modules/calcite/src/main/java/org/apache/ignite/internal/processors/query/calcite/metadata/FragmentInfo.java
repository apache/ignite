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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.splitter.RelSource;

/**
 *
 */
public class FragmentInfo {
    private final NodesMapping mapping;
    private final ImmutableList<Pair<IgniteReceiver, RelSource>> sources;

    public FragmentInfo(Pair<IgniteReceiver, RelSource> source) {
        this(ImmutableList.of(source), null);
    }

    public FragmentInfo(NodesMapping mapping) {
        this(null, mapping);
    }

    public FragmentInfo(ImmutableList<Pair<IgniteReceiver, RelSource>> sources, NodesMapping mapping) {
        this.sources = sources;
        this.mapping = mapping;
    }

    public NodesMapping mapping() {
        return mapping;
    }

    public ImmutableList<Pair<IgniteReceiver, RelSource>> sources() {
        return sources;
    }

    public FragmentInfo merge(FragmentInfo other) throws LocationMappingException {
        return new FragmentInfo(
            merge(sources(), other.sources()),
            merge(mapping(), other.mapping()));
    }

    private static NodesMapping merge(NodesMapping left, NodesMapping right) throws LocationMappingException {
        if (left == null)
            return right;
        if (right == null)
            return left;

        return left.mergeWith(right);
    }

    private static <T> ImmutableList<T> merge(ImmutableList<T> left, ImmutableList<T> right) {
        if (left == null)
            return right;
        if (right == null)
            return left;

        return ImmutableList.<T>builder().addAll(left).addAll(right).build();
    }
}
