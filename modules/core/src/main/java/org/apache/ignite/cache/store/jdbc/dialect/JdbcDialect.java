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

package org.apache.ignite.cache.store.jdbc.dialect;

import java.io.Serializable;
import java.util.Collection;

/**
 * Represents a dialect of SQL implemented by a particular RDBMS.
 */
public interface JdbcDialect extends Serializable {
    /**
     * @param ident SQL identifier to escape.
     * @return Escaped SQL identifier.
     */
    public String escape(String ident);

    /**
     * Construct query to get ranges bounds.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns for order.
     * @return Query for select count.
     */
    public String loadCacheSelectRangeQuery(String fullTblName, Collection<String> keyCols);

    /**
     * Construct load cache query over specified range.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns for order.
     * @param uniqCols Database unique value columns.
     * @param appendLowerBound Need add lower bound for range.
     * @param appendUpperBound Need add upper bound for range.
     * @return Query for select count.
     */
    public String loadCacheRangeQuery(String fullTblName, Collection<String> keyCols, Iterable<String> uniqCols,
        boolean appendLowerBound, boolean appendUpperBound);

    /**
     * Construct load cache query.
     *
     * @param fullTblName Full table name.
     * @param uniqCols Database unique value columns.
     * @return Load cache query.
     */
    public String loadCacheQuery(String fullTblName, Iterable<String> uniqCols);

    /**
     * Construct load query.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns.
     * @param cols Selected columns.
     * @param keyCnt Key count.
     * @return Load query.
     */
    public String loadQuery(String fullTblName, Collection<String> keyCols, Iterable<String> cols,
        int keyCnt);

    /**
     * Construct insert query.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns.
     * @param valCols Database value columns.
     * @return Insert query.
     */
    public String insertQuery(String fullTblName, Collection<String> keyCols, Collection<String> valCols);

    /**
     * Construct update query.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns.
     * @param valCols Database value columns.
     * @return Update query.
     */
    public String updateQuery(String fullTblName, Collection<String> keyCols, Iterable<String> valCols);

    /**
     * @return {@code True} if database support merge operation.
     */
    public boolean hasMerge();

    /**
     * Construct merge query.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns.
     * @param uniqCols Database unique value columns.
     * @return Merge query.
     */
    public String mergeQuery(String fullTblName, Collection<String> keyCols, Collection<String> uniqCols);

    /**
     * Construct remove query.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns.
     * @return Remove query.
     */
    public String removeQuery(String fullTblName, Iterable<String> keyCols);

    /**
     * Get max query parameters count.
     *
     * @return Max query parameters count.
     */
    public int getMaxParameterCount();

    /**
     * Gives the JDBC driver a hint how many rows should be fetched from the database when more rows are needed.
     * If the value specified is zero, then the hint is ignored.
     * The default value is zero.
     *
     * @return The fetch size for result sets.
     */
    public int getFetchSize();
}
