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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.result.RowFactory;
import org.h2.value.Value;

/**
 * Row factory.
 */
public class GridH2RowFactory extends RowFactory {
    /**
     * @param v Value.
     * @return Row.
     */
    public static GridH2Row create(Value v) {
        return new RowKey(v);
    }

    /**
     * @param v1 Value 1.
     * @param v2 Value 2.
     * @return Row.
     */
    public static GridH2Row create(Value v1, Value v2) {
        return new RowPair(v1, v2);
    }

    /**
     * @param data Values.
     * @return Row.
     */
    public static GridH2Row create(Value... data) {
        switch (data.length) {
            case 0:
                throw new IllegalStateException("Zero columns row.");

            case 1:
                return new RowKey(data[0]);

            case 2:
                return new RowPair(data[0], data[1]);

            default:
                return new RowSimple(data);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row createRow(Value[] data, int memory) {
        return create(data);
    }

    /**
     * Single value row.
     */
    private static final class RowKey extends GridH2Row {
        /** */
        private Value key;

        /**
         * @param key Key.
         */
        public RowKey(Value key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            assert idx == 0 : idx;
            return key;
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            assert idx == 0 : idx;
            key = v;
        }
    }

    /**
     * Row of two values.
     */
    private static final class RowPair extends GridH2Row  {
        /** */
        private Value v1;

        /** */
        private Value v2;

        /**
         * @param v1 First value.
         * @param v2 Second value.
         */
        private RowPair(Value v1, Value v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return 2;
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            return idx == 0 ? v1 : v2;
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            if (idx == 0)
                v1 = v;
            else {
                assert idx == 1 : idx;

                v2 = v;
            }
        }
    }

    /**
     * Simple array based row.
     */
    private static final class RowSimple extends GridH2Row {
        /** */
        @GridToStringInclude
        private Value[] vals;

        /**
         * @param vals Values.
         */
        private RowSimple(Value[] vals) {
            this.vals = vals;
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return vals.length;
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            return vals[idx];
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            vals[idx] = v;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RowSimple.class, this);
        }
    }
}
