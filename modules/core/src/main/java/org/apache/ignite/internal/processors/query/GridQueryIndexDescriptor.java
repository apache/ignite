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
import org.apache.ignite.cache.IndexFieldOrder;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Describes an index to be created for a certain type. It contains all necessary
 * information about fields, order, uniqueness, and specified
 * whether this is SQL or Text index.
 * See also {@link GridQueryTypeDescriptor#indexes()}.
 */
public class GridQueryIndexDescriptor {
    /** Fields sorted by order number. */
    private final Collection<T2<String, Integer>> fields = new TreeSet<>(
        new Comparator<T2<String, Integer>>() {
            @Override public int compare(T2<String, Integer> o1, T2<String, Integer> o2) {
                if (o1.get2().equals(o2.get2())) // Order is equal, compare field names to avoid replace in Set.
                    return o1.get1().compareTo(o2.get1());

                return o1.get2() < o2.get2() ? -1 : 1;
            }
        });

    /** Index name. */
    private final String name;

    /** Index type. */
    private final QueryIndexType type;

    /** Inline size. */
    private final int inlineSize;

    /** Fields which should be indexed in descending order. */
    private Collection<String> descendings;

    /** Fields which should store nulls last. */
    private Collection<String> nullsLast;

    /** Fields which should store nulls first. */
    private Collection<String> nullsFirst;

    /**
     * @param name Index name.
     * @param type Index Type.
     * @param inlineSize Inline size.
     */
    public GridQueryIndexDescriptor(String name, QueryIndexType type, int inlineSize) {
        assert type != null;

        this.name = name;
        this.type = type;
        this.inlineSize = inlineSize;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * Gets all fields to be indexed.
     *
     * @return Fields to be indexed.
     */
    public Collection<String> fields() {
        Collection<String> res = new ArrayList<>(fields.size());

        for (T2<String, Integer> t : fields)
            res.add(t.get1());

        return res;
    }

    /**
     * Adds field to this index.
     *
     * @param field Field name.
     * @param orderNum Field order number in this index.
     * @param order Field order.
     */
    public void addField(String field, int orderNum, IndexFieldOrder order) {
        fields.add(new T2<>(field, orderNum));

        if (order.isDescending()) {
            if (descendings == null)
                descendings = new HashSet<>();

            descendings.add(field);
        }

        if (order.isNullsLast()) {
            if (nullsLast == null)
                nullsLast = new HashSet<>();

            nullsLast.add(field);
        }

        if (order.isNullsFirst()) {
            if (nullsFirst == null)
                nullsFirst = new HashSet<>();

            nullsFirst.add(field);
        }
    }

    /**
     * Specifies order of the index for each indexed field.
     *
     * @param field Field name.
     * @return {@code True} if given field should be indexed in descending order.
     */
    public boolean descending(String field) {
        return descendings != null && descendings.contains(field);
    }

    /**
     * Whether {@code null} should be stored after other values for the specified index field.
     *
     * @param field Field name.
     * @return {@code True} if nulls should be indexed after other values.
     */
    public boolean nullsLast(String field) {
        return nullsLast != null && nullsLast.contains(field);
    }

    /**
     * Whether {@code null} should be stored before other values for the specified index field.
     *
     * @param field Field name.
     * @return {@code True} if nulls should be indexed before other values.
     */
    public boolean nullsFirst(String field) {
        return nullsFirst != null && nullsFirst.contains(field);
    }

    /**
     * Gets index type.
     *
     * @return Type.
     */
    public QueryIndexType type() {
        return type;
    }

    /**
     * Gets inline size for SORTED index.
     *
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }
}
