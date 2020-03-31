/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import org.h2.api.AggregateFunction;

/**
 *  SQL aggregate function to select the first or last value in the sorted group.
 */
public class GridAggregateOrderedFunction implements AggregateFunction {
    /** */
    protected Object[] currentVal;

    /** Ascending order. */
    protected boolean asc;

    /** Nulls-last comparator. */
    protected Comparator<? super Comparable> comparator;

    /**
     * @param comparator comparator.
     * @param asc ascending order.
     */
    public GridAggregateOrderedFunction(boolean asc, Comparator<? super Comparable> comparator ) {
        this.asc = asc;
        this.comparator = comparator;
    }

    /** {@inheritDoc}  */
    @Override public void init(Connection conn) throws SQLException {

    }

    /** {@inheritDoc}  */
    @Override  public int getType(int[] inputTypes) throws SQLException {
        if (inputTypes.length < 2)
            throw new SQLException("Aggregation function should have at least two arguments.");

        return inputTypes[0];
    }

    /** {@inheritDoc}  */
    @Override public void add(Object value) throws SQLException {
        Object[] arr = value instanceof Object[] ? (Object[]) value : new Object[]{value};

        if (currentVal == null) {
            currentVal = arr;

            return;
        }

        int compare = (asc ? 1 : -1) * compare(currentVal, arr);

        if (compare > 0)
            currentVal = arr;
    }

    /**
     * Compares two arrays starting with the first element.
     * Elements of arrays should implement {@link Comparable} interface.
     *
     * @param arg1 the first array.
     * @param arg2 the second arrays.
     * @return comparison result.
     * @throws SQLException if failed.
     */
    protected int compare(Object[] arg1, Object[] arg2) throws SQLException {
        int result = 0;

        for (int i = 1; i < arg1.length; i++) {
            int compare = comparator.compare((Comparable)arg1[i], (Comparable)arg2[i]);

            if (compare < 0) {
                result = -1;

                break;
            }
            else if (compare > 0) {
                result = 1;

                break;
            }
        }

        return result;
    }

    /** {@inheritDoc}  */
    @Override public Object getResult() throws SQLException {
        if (currentVal == null)
            return null;

        return currentVal[0];
    }
}
