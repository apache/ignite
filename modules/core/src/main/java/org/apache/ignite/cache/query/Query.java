/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.query;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Base class for all Ignite cache queries.
 * Use {@link SqlQuery} and {@link TextQuery} for SQL and
 * text queries accordingly.
 *
 * @see IgniteCache#query(Query)
 */
public abstract class Query<R> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default query page size. */
    public static final int DFLT_PAGE_SIZE = 1024;

    /** Page size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Local flag. */
    private boolean loc;

    /**
     * Empty constructor.
     */
    Query() {
        // No-op.
    }

    /**
     * Gets optional page size, if {@code 0}, then default is used.
     *
     * @return Optional page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets optional page size, if {@code 0}, then default is used.
     *
     * @param pageSize Optional page size.
     * @return {@code this} for chaining.
     */
    public Query<R> setPageSize(int pageSize) {
        if (pageSize <= 0)
            throw new IllegalArgumentException("Page size must be above zero.");

        this.pageSize = pageSize;

        return this;
    }

    /**
     * Returns {@code true} if this query should be executed on local node only.
     *
     * @return Local flag.
     */
    public boolean isLocal() {
        return loc;
    }

    /**
     * Sets whether this query should be executed on local node only.
     *
     * @param loc Local flag.
     * @return {@code this} for chaining.
     */
    public Query<R> setLocal(boolean loc) {
        this.loc = loc;

        return this;
    }

    /**
     * Prepares the partitions.
     *
     * @param parts Partitions.
     */
    protected int[] prepare(int[] parts) {
        if (parts == null)
            return null;

        A.notEmpty(parts, "Partitions");

        boolean sorted = true;

        // Try to do validation in one pass, if array is already sorted.
        for (int i = 0; i < parts.length; i++) {
            if (i < parts.length - 1)
                if (parts[i] > parts[i + 1])
                    sorted = false;
                else if (sorted)
                    validateDups(parts[i], parts[i + 1]);

            A.ensure(0 <= parts[i] && parts[i] < CacheConfiguration.MAX_PARTITIONS_COUNT, "Illegal partition");
        }

        // Sort and validate again.
        if (!sorted) {
            Arrays.sort(parts);

            for (int i = 0; i < parts.length; i++) {
                if (i < parts.length - 1)
                    validateDups(parts[i], parts[i + 1]);
            }
        }

        return parts;
    }

    /**
     * @param p1 Part 1.
     * @param p2 Part 2.
     */
    private void validateDups(int p1, int p2) {
        A.ensure(p1 != p2, "Partition duplicates are not allowed: " + p1);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Query.class, this);
    }
}