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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Index descriptor.
 */
public class QueryIndexDescriptorImpl implements GridQueryIndexDescriptor {
    /** Fields sorted by order number. */
    private final Collection<T2<String, Integer>> fields = new TreeSet<>(
        new Comparator<T2<String, Integer>>() {
            @Override public int compare(T2<String, Integer> o1, T2<String, Integer> o2) {
                if (o1.get2().equals(o2.get2())) // Order is equal, compare field names to avoid replace in Set.
                    return o1.get1().compareTo(o2.get1());

                return o1.get2() < o2.get2() ? -1 : 1;
            }
        });

    /** Fields which should be indexed in descending order. */
    private Collection<String> descendings;

    /** Type descriptor. */
    @GridToStringExclude
    private final QueryTypeDescriptorImpl typDesc;

    /** Index name. */
    private final String name;

    /** */
    private final QueryIndexType type;

    /** */
    private final int inlineSize;

    /**
     * Constructor.
     *
     * @param typDesc Type descriptor.
     * @param name Index name.
     * @param type Type.
     * @param inlineSize Inline size.
     */
    public QueryIndexDescriptorImpl(QueryTypeDescriptorImpl typDesc, String name, QueryIndexType type, int inlineSize) {
        assert type != null;

        this.typDesc = typDesc;
        this.name = name;
        this.type = type;
        this.inlineSize = inlineSize;
    }

    /**
     * @return Type descriptor.
     */
    public QueryTypeDescriptorImpl typeDescriptor() {
        return typDesc;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> fields() {
        Collection<String> res = new ArrayList<>(fields.size());

        for (T2<String, Integer> t : fields)
            res.add(t.get1());

        return res;
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public boolean descending(String field) {
        return descendings != null && descendings.contains(field);
    }

    /**
     * Adds field to this index.
     *
     * @param field Field name.
     * @param orderNum Field order number in this index.
     * @param descending Sort order.
     * @return This instance for chaining.
     * @throws IgniteCheckedException If failed.
     */
    public QueryIndexDescriptorImpl addField(String field, int orderNum, boolean descending)
        throws IgniteCheckedException {
        if (!typDesc.hasField(field))
            throw new IgniteCheckedException("Field not found: " + field);

        fields.add(new T2<>(field, orderNum));

        if (descending) {
            if (descendings == null)
                descendings = new HashSet<>();

            descendings.add(field);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public QueryIndexType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexDescriptorImpl.class, this);
    }
}
