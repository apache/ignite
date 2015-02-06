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

package org.apache.ignite.internal.processors.query.h2.twostep.messages;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.h2.store.*;
import org.h2.value.*;

import java.io.*;
import java.util.*;

/**
 * Next page response.
 */
public class GridNextPageResponse implements Externalizable {
    /** */
    private long qryReqId;

    /** */
    private int qry;

    /** */
    private int page;

    /** */
    private int allRows;

    /** */
    private Collection<Value[]> rows;

    /** */
    private boolean last;

    /**
     * For {@link Externalizable}.
     */
    public GridNextPageResponse() {
        // No-op.
    }

    /**
     * @param qryReqId Query request ID.
     * @param qry Query.
     * @param page Page.
     * @param allRows All rows count.
     * @param last Last row.
     * @param rows Rows.
     */
    public GridNextPageResponse(long qryReqId, int qry, int page, int allRows, boolean last, Collection<Value[]> rows) {
        assert rows != null;

        this.qryReqId = qryReqId;
        this.qry = qry;
        this.page = page;
        this.allRows = allRows;
        this.last = last;
        this.rows = rows;
    }

    /**
     * @return Query request ID.
     */
    public long queryRequestId() {
        return qryReqId;
    }

    /**
     * @return Query.
     */
    public int query() {
        return qry;
    }

    /**
     * @return Page.
     */
    public int page() {
        return page;
    }

    /**
     * @return All rows.
     */
    public int allRows() {
        return allRows;
    }

    /**
     * @return {@code true} If this is the last page.
     */
    public boolean isLast() {
        return last;
    }

    /**
     * @return Rows.
     */
    public Collection<Value[]> rows() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(qryReqId);
        out.writeInt(qry);
        out.writeInt(page);
        out.writeBoolean(last);
        out.writeInt(allRows);

        out.writeInt(rows.size());

        if (rows.isEmpty())
            return;

        Data data = Data.create(null, 512);

        boolean first = true;

        for (Value[] row : rows) {
            if (first) {
                out.writeInt(row.length);

                first = false;
            }

            for (Value val : row)
                data.writeValue(val);
        }

        out.writeInt(data.length());
        out.write(data.getBytes(), 0, data.length());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        qryReqId = in.readLong();
        qry = in.readInt();
        page = in.readInt();
        last = in.readBoolean();
        allRows = in.readInt();

        int rowCnt = in.readInt();

        if (rowCnt == 0)
            rows = Collections.emptyList();
        else {
            rows = new ArrayList<>(rowCnt);

            int cols = in.readInt();
            int dataSize = in.readInt();

            byte[] dataBytes = new byte[dataSize];

            in.readFully(dataBytes);

            Data data = Data.create(null, dataBytes);

            for (int r = 0; r < rowCnt; r++) {
                Value[] row = new Value[cols];

                for (int c = 0; c < cols; c++)
                    row[c] = data.readValue();

                rows.add(row);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNextPageResponse.class, this);
    }
}
