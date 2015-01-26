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

package org.apache.ignite.cache.store.jdbc.dialect;

import java.util.*;

/**
 * Represents a dialect of SQL implemented by a particular RDBMS.
 */
public interface JdbcDialect {
    /**
     * Construct load cache query.
     *
     * @param schema Database schema name.
     * @param tblName Database table name.
     * @param uniqCols Database unique value columns.
     * @return Load cache query.
     */
    public String loadCacheQuery(String schema, String tblName, Iterable<String> uniqCols);

    /**
     * Construct load query.
     *
     * @param schema Database schema name.
     * @param tblName Database table name.
     * @param keyCols Database key columns.
     * @param cols Selected columns.
     * @param keyCnt Key count.
     * @return Load query.
     */
    public String loadQuery(String schema, String tblName, Collection<String> keyCols, Iterable<String> cols,
        int keyCnt);

    /**
     * Construct insert query.
     *
     * @param schema Database schema name.
     * @param tblName Database table name.
     * @param keyCols Database key columns.
     * @param valCols Database value columns.
     */
    public String insertQuery(String schema, String tblName, Collection<String> keyCols, Collection<String> valCols);

    /**
     * Construct update query.
     *
     * @param schema Database schema name.
     * @param tblName Database table name.
     * @param keyCols Database key columns.
     * @param valCols Database value columns.
     */
    public String updateQuery(String schema, String tblName, Collection<String> keyCols, Iterable<String> valCols);

    /**
     * @return {@code True} if database support merge operation.
     */
    public boolean hasMerge();

    /**
     * Construct merge query.
     *
     * @param schema Database schema name.
     * @param tblName Database table name.
     * @param keyCols Database key columns.
     * @param uniqCols Database unique value columns.
     * @return Put query.
     */
    public String mergeQuery(String schema, String tblName, Collection<String> keyCols, Collection<String> uniqCols);

    /**
     * Construct remove query.
     *
     * @param schema Database schema name.
     * @param tblName Database table name.
     * @param keyCols Database key columns.
     * @return Remove query.
     */
    public String removeQuery(String schema, String tblName, Iterable<String> keyCols);

    /**
     * Get max query parameters count.
     *
     * @return Max query parameters count.
     */
    public int getMaxParamsCnt();
}
