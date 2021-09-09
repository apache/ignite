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

package org.apache.ignite.cache;

import java.io.Serializable;
import java.util.Objects;

/**
 * Describes order of an indexed field.
 *
 * Note, in case both {@link #nullsFirst} and {@link #nullsLast} are set to false it is considered that {@code null} is
 * less than any non-null value. It means that nulls are stored before all values for ASC sort, and after for DESC.
 */
public class IndexFieldOrder implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** If set to {@code true} then sort order is ASC, otherwise DESC. */
    private final boolean asc;

    /** If set {@code true} then nulls should be indexed after other values. */
    private final boolean nullsLast;

    /** If set {@code true} then nulls should be indexed before other values. */
    private final boolean nullsFirst;

    /**
     * Default constructor.
     */
    public IndexFieldOrder() {
        this(true, false, false);
    }

    /**
     * Constructor that sets sort type.
     *
     * @param asc Sort order. {@code true} for ASC and {@code false} for DESC.
     */
    public IndexFieldOrder(boolean asc) {
        this(asc, false, false);
    }

    /**
     * Constructor.
     *
     * @param asc Sort order. {@code true} for ASC and {@code false} for DESC.
     * @param nullsFirst {@code null} should be stored before any other values despite sort type.
     * @param nullsLast {@code null} should be stored after any other values despite sort type.
     */
    public IndexFieldOrder(boolean asc, boolean nullsFirst, boolean nullsLast) {
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.nullsLast = nullsLast;
    }

    /**
     * @return Whether sort type is ascending.
     */
    public boolean isAscending() {
        return asc;
    }

    /**
     * @return Whether sort type is descending.
     */
    public boolean isDescending() {
        return !asc;
    }

    /**
     * @return Whether nulls order is "nulls last".
     */
    public boolean isNullsLast() {
        return nullsLast;
    }

    /**
     * @return Whether nulls order is "nulls first".
     */
    public boolean isNullsFirst() {
        return nullsFirst;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        IndexFieldOrder order = (IndexFieldOrder)o;
        return asc == order.asc && nullsLast == order.nullsLast && nullsFirst == order.nullsFirst;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(asc, nullsLast, nullsFirst);
    }
}
