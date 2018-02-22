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

package org.apache.ignite.ml.trees.trainers.columnbased;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * Class representing a simple index in 2d matrix in the form (row, col).
 */
public class BiIndex implements Externalizable {
    /** Row. */
    private int row;

    /** Column. */
    @AffinityKeyMapped
    private int col;

    /**
     * No-op constructor for serialization/deserialization.
     */
    public BiIndex() {
        // No-op.
    }

    /**
     * Construct BiIndex from row and column.
     *
     * @param row Row.
     * @param col Column.
     */
    public BiIndex(int row, int col) {
        this.row = row;
        this.col = col;
    }

    /**
     * Returns row.
     *
     * @return Row.
     */
    public int row() {
        return row;
    }

    /**
     * Returns column.
     *
     * @return Column.
     */
    public int col() {
        return col;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BiIndex idx = (BiIndex)o;

        if (row != idx.row)
            return false;
        return col == idx.col;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = row;
        res = 31 * res + col;
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "BiIndex [" +
            "row=" + row +
            ", col=" + col +
            ']';
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(row);
        out.writeInt(col);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        row = in.readInt();
        col = in.readInt();
    }
}
