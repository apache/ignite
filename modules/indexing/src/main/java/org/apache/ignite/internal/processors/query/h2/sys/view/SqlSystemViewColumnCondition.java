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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Column condition.
 */
public class SqlSystemViewColumnCondition {
    /** Is equality. */
    private final boolean isEquality;

    /** Is range. */
    private final boolean isRange;

    /** Value 1. */
    private final Value val;

    /**
     * @param isEquality Is equality.
     * @param isRange Is range.
     * @param val Value for equality.
     */
    private SqlSystemViewColumnCondition(boolean isEquality, boolean isRange, Value val) {
        this.isEquality = isEquality;
        this.isRange = isRange;
        this.val = val;
    }

    /**
     * Parse condition for column.
     *
     * @param colIdx Column index.
     * @param start Start row values.
     * @param end End row values.
     */
    public static SqlSystemViewColumnCondition forColumn(int colIdx, SearchRow start, SearchRow end) {
        boolean isEquality = false;
        boolean isRange = false;

        Value val = null;
        Value val2 = null;

        if (start != null && colIdx >= 0 && colIdx < start.getColumnCount())
            val = start.getValue(colIdx);

        if (end != null && colIdx >= 0 && colIdx < end.getColumnCount())
            val2 = end.getValue(colIdx);

        if (val != null && val2 != null) {
            if (val.equals(val2))
                isEquality = true;
            else
                isRange = true;
        }
        else if (val != null || val2 != null)
            isRange = true;

        return new SqlSystemViewColumnCondition(isEquality, isRange, val);
    }

    /**
     * Checks whether the condition is equality.
     */
    public boolean isEquality() {
        return isEquality;
    }

    /**
     * Checks whether the condition is range.
     */
    public boolean isRange() {
        return isRange;
    }

    /**
     * Gets value, if condition is equality.
     */
    public Value valueForEquality() {
        if (isEquality)
            return val;

        return null;
    }
}
