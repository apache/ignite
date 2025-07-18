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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.agg;

import java.util.Objects;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/**
 *
 */
public class GroupKey<Row> implements Binarylizable {
    /** */
    private Row row;

    /** */
    private RowHandler<Row> hnd;

    /** */
    public GroupKey(Row row, RowHandler<Row> hnd) {
        this.row = row;
        this.hnd = hnd;
    }

    /** */
    public Row row() {
        return row;
    }

    /** */
    public RowHandler<Row> rowHandler() {
        return hnd;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter rawWriter = writer.rawWriter();

        int colCnt = hnd.columnCount(row);

        rawWriter.writeInt(colCnt);

        for (int i = 0; i < colCnt; i++)
            rawWriter.writeObject(hnd.get(i, row));
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader rawReader = reader.rawReader();

        int colCnt = rawReader.readInt();

        Object[] row0 = new Object[colCnt];

        for (int i = 0; i < colCnt; i++)
            row0[i] = rawReader.readObject();

        row = (Row)row0;
        hnd = (RowHandler<Row>)ArrayRowHandler.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GroupKey<Row> other = (GroupKey<Row>)o;

        int colCnt = hnd.columnCount(row);

        if (colCnt != other.hnd.columnCount(other.row))
            return false;

        for (int i = 0; i < colCnt; i++) {
            if (!Objects.equals(hnd.get(i, row), other.hnd.get(i, other.row)))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int hashCode = 0;

        for (int i = 0; i < hnd.columnCount(row); i++)
            hashCode = hashCode * 31 + Objects.hashCode(hnd.get(i, row));

        return hashCode;
    }
}
