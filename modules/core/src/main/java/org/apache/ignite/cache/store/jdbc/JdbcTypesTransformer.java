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

package org.apache.ignite.cache.store.jdbc;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;

/**
 * API for implementing custom mapping logic for loaded from store data.
 */
public interface JdbcTypesTransformer extends Serializable {
    /** Numeric types. */
    public final List<Integer> NUMERIC_TYPES =
        U.sealList(TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE, NUMERIC, DECIMAL);


    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object and
     * will convert to the requested Java data type.
     *
     * @param rs Result set.
     * @param colIdx Column index in result set.
     * @param type Class representing the Java data type to convert the designated column to.
     * @return Value in column.
     * @throws SQLException If a database access error occurs or this method is called.
     */
    public Object getColumnValue(ResultSet rs, int colIdx, Class<?> type) throws SQLException;
}
