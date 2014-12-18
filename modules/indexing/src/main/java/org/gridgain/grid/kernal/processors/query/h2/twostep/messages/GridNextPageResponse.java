/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.twostep.messages;

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

    /**
     * @param qryReqId Query request ID.
     * @param qry Query.
     * @param page Page.
     * @param allRows All rows count.
     * @param rows Rows.
     */
    public GridNextPageResponse(long qryReqId, int qry, int page, int allRows, Collection<Value[]> rows) {
        assert rows != null;

        this.qryReqId = qryReqId;
        this.qry = qry;
        this.page = page;
        this.allRows = allRows;
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
        allRows = in.readInt();

        int rowCnt = in.readInt();

        if (rowCnt == 0)
            rows = Collections.emptyList();
        else {
            rows = new ArrayList<>(rowCnt);

            int cols = in.readInt();
            int dataSize = in.readInt();

            Data data = Data.create(null, dataSize);

            for (int r = 0; r < rowCnt; r++) {
                Value[] row = new Value[cols];

                for (int c = 0; c < cols; c++)
                    row[c] = data.readValue();

                rows.add(row);
            }
        }
    }
}
