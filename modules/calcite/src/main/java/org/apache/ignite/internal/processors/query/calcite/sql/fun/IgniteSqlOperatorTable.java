/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

/**
 * Operator table that contains only Ignite-specific functions and operators.
 */
public class IgniteSqlOperatorTable extends ReflectiveSqlOperatorTable {
    /**
     * The table of contains Ignite-specific operators.
     */
    private static IgniteSqlOperatorTable instance;

    /**
     *
     */
    public static final SqlFunction LENGTH =
            new SqlFunction(
                    "LENGTH",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.CHARACTER,
                    SqlFunctionCategory.NUMERIC);

    /**
     *
     */
    public static final SqlFunction SYSTEM_RANGE = new SqlSystemRangeFunction();

    /**
     * Returns the Ignite operator table, creating it if necessary.
     */
    public static synchronized IgniteSqlOperatorTable instance() {
        if (instance == null) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the
            // table until the constructor of the sub-class has completed.
            instance = new IgniteSqlOperatorTable();
            instance.init();
        }
        return instance;
    }
}
