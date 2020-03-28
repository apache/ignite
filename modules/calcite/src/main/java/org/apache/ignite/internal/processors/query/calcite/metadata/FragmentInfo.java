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
import org.apache.ignite.internal.processors.query.calcite.prepare.RelTargetAware;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Collects meta information about a query fragment, mainly it is data
 * location and a list of nodes, capable to execute the fragment on.
 */
public class FragmentInfo {
    /** */
    private final NodesMapping mapping;

    /** */
    private final ImmutableList<RelTargetAware> targetAwareList;

    /**
     * Constructs Values leaf fragment info.
     */
    public FragmentInfo() {
        this(ImmutableList.of(), null);
    }

    /**
     * Constructs Receiver leaf fragment info.
     *
     * @param targetAware A node that needs target information.
     */
    public FragmentInfo(RelTargetAware targetAware) {
        this(ImmutableList.of(targetAware), null);
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
     * @param targetAwareList nodes that need target information.
     * @param mapping Nodes mapping, describing where interested data placed.
     */
    public FragmentInfo(ImmutableList<RelTargetAware> targetAwareList, NodesMapping mapping) {
        this.targetAwareList = targetAwareList;
        this.mapping = mapping;
    }

    /**
     * @return Nodes mapping, describing where interested data placed.
     */
    public NodesMapping mapping() {
        return mapping;
    }

    /**
     * @return {@code True} if the fragment has nodes mapping.
     */
    public boolean mapped() {
        return mapping != null;
    }

    /**
     * @return Nodes that need target information..
     */
    public ImmutableList<RelTargetAware> targetAwareList() {
        return targetAwareList;
    }

    /**
     * Merges two FragmentInfo objects.
     * @param other FragmentInfo to merge with.
     * @return Resulting FragmentInfo.
     * @throws LocationMappingException in case there is no nodes capable to execute a query fragment.
     */
    public FragmentInfo merge(FragmentInfo other) throws LocationMappingException {
        return new FragmentInfo(
            merge(targetAwareList(), other.targetAwareList()),
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

            if (newMapping != mapping)
                return new FragmentInfo(targetAwareList, newMapping);
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
        if (F.isEmpty(left))
            return right;
        if (F.isEmpty(right))
            return left;

        return ImmutableList.<T>builder().addAll(left).addAll(right).build();
    }
}
