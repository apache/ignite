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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.splitter.RelSource;

/**
 * Collects meta information about a query fragment, mainly it is data
 * location and a list of nodes, capable to execute the fragment on.
 */
public class FragmentInfo {
    /** */
    private final NodesMapping mapping;

    /** */
    private final ImmutableList<Pair<IgniteReceiver, RelSource>> sources;

    /**
     * Constructs Receiver leaf fragment info.
     *
     * @param source Pair of a Receiver relational node and its data source information.
     */
    public FragmentInfo(Pair<IgniteReceiver, RelSource> source) {
        this(ImmutableList.of(source), null);
    }

    /**
     * Constructs Scan leaf fragment info.
     *
     * @param mapping Nodes mapping, describing where interested data placed.
     */
    public FragmentInfo(NodesMapping mapping) {
        this(null, mapping);
    }

    /**
     * Used on merge of two relational node tree edges.
     *
     * @param sources Pairs of underlying Receiver relational nodes and theirs data source information.
     * @param mapping Nodes mapping, describing where interested data placed.
     */
    public FragmentInfo(ImmutableList<Pair<IgniteReceiver, RelSource>> sources, NodesMapping mapping) {
        this.sources = sources;
        this.mapping = mapping;
    }

    /**
     * @return Nodes mapping, describing where interested data placed.
     */
    public NodesMapping mapping() {
        return mapping;
    }

    /**
     * @return Pairs of underlying Receiver relational nodes and theirs data source information.
     */
    public ImmutableList<Pair<IgniteReceiver, RelSource>> sources() {
        return sources;
    }

    /**
     * Merges two FragmentInfo objects.
     * @param other FragmentInfo to merge with.
     * @return Resulting FragmentInfo.
     * @throws LocationMappingException in case there is no nodes capable to execute a query fragment.
     */
    public FragmentInfo merge(FragmentInfo other) throws LocationMappingException {
        return new FragmentInfo(
            merge(sources(), other.sources()),
            merge(mapping(), other.mapping()));
    }

    /**
     * Prunes involved partitions (hence nodes, involved in query execution) on the basis of filter,
     * its distribution, query parameters and original nodes mapping.
     * @param filter Filter.
     * @return Resulting fragment info.
     */
    public FragmentInfo prune(IgniteFilter filter) {
        if (mapping != null) {
            NodesMapping newMapping = mapping.prune(filter);

            if (newMapping != mapping) {
                return new FragmentInfo(sources, newMapping);
            }
        }

        return this;
    }

    /** */
    private static NodesMapping merge(NodesMapping left, NodesMapping right) throws LocationMappingException {
        if (left == null)
            return right;
        if (right == null)
            return left;

        return left.mergeWith(right);
    }

    /** */
    private static <T> ImmutableList<T> merge(ImmutableList<T> left, ImmutableList<T> right) {
        if (left == null)
            return right;
        if (right == null)
            return left;

        return ImmutableList.<T>builder().addAll(left).addAll(right).build();
    }
}
