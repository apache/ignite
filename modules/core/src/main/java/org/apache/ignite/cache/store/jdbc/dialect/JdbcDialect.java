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

import java.io.*;
import java.util.*;

/**
 * Represents a dialect of SQL implemented by a particular RDBMS.
 */
public interface JdbcDialect extends Serializable {
    /**
     * Construct select count query.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns for order.
     * @return Query for select count.
     */
    public String loadCacheSelectRangeQuery(String fullTblName, Collection<String> keyCols);

    /**
     * Construct select count query.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns for order.
     * @param uniqCols Database unique value columns.
     * @param appendLowerBound Need add lower bound for range.
     * @param appendUpperBound Need add upper bound for range.
     * @return Query for select count.
     */
    public String loadCacheRangeQuery(String fullTblName,
        Collection<String> keyCols, Iterable<String> uniqCols, boolean appendLowerBound, boolean appendUpperBound);

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
     */
    public String insertQuery(String fullTblName, Collection<String> keyCols, Collection<String> valCols);

    /**
     * Construct update query.
     *
     * @param fullTblName Full table name.
     * @param keyCols Database key columns.
     * @param valCols Database value columns.
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
     * @return Put query.
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
}
