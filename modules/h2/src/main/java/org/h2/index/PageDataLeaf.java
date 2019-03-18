/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.value.Value;

/**
 * A leaf page that contains data of one or multiple rows. Format:
 * <ul>
 * <li>page type: byte (0)</li>
 * <li>checksum: short (1-2)</li>
 * <li>parent page id (0 for root): int (3-6)</li>
 * <li>table id: varInt</li>
 * <li>column count: varInt</li>
 * <li>entry count: short</li>
 * <li>with overflow: the first overflow page id: int</li>
 * <li>list of key / offset pairs (key: varLong, offset: shortInt)</li>
 * <li>data</li>
 * </ul>
 */
public class PageDataLeaf extends PageData {

    private final boolean optimizeUpdate;

    /**
     * The row offsets.
     */
    private int[] offsets;

    /**
     * The rows.
     */
    private Row[] rows;

    /**
     * For pages with overflow: the soft reference to the row
     */
    private SoftReference<Row> rowRef;

    /**
     * The page id of the first overflow page (0 if no overflow).
     */
    private int firstOverflowPageId;

    /**
     * The start of the data area.
     */
    private int start;

    /**
     * The size of the row in bytes for large rows.
     */
    private int overflowRowSize;

    private int columnCount;

    private int memoryData;

    private boolean writtenData;

    private PageDataLeaf(PageDataIndex index, int pageId, Data data) {
        super(index, pageId, data);
        this.optimizeUpdate = index.getDatabase().getSettings().optimizeUpdate;
    }

    /**
     * Create a new page.
     *
     * @param index the index
     * @param pageId the page id
     * @param parentPageId the parent
     * @return the page
     */
    static PageDataLeaf create(PageDataIndex index, int pageId, int parentPageId) {
        PageDataLeaf p = new PageDataLeaf(index, pageId, index.getPageStore()
                .createData());
        index.getPageStore().logUndo(p, null);
        p.rows = Row.EMPTY_ARRAY;
        p.parentPageId = parentPageId;
        p.columnCount = index.getTable().getColumns().length;
        p.writeHead();
        p.start = p.data.length();
        return p;
    }

    /**
     * Read a data leaf page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageDataIndex index, Data data, int pageId) {
        PageDataLeaf p = new PageDataLeaf(index, pageId, data);
        p.read();
        return p;
    }

    private void read() {
        data.reset();
        int type = data.readByte();
        data.readShortInt();
        this.parentPageId = data.readInt();
        int tableId = data.readVarInt();
        if (tableId != index.getId()) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected table:" + index.getId() +
                    " got:" + tableId + " type:" + type);
        }
        columnCount = data.readVarInt();
        entryCount = data.readShortInt();
        offsets = new int[entryCount];
        keys = new long[entryCount];
        rows = new Row[entryCount];
        if (type == Page.TYPE_DATA_LEAF) {
            if (entryCount != 1) {
                DbException.throwInternalError("entries: " + entryCount);
            }
            firstOverflowPageId = data.readInt();
        }
        for (int i = 0; i < entryCount; i++) {
            keys[i] = data.readVarLong();
            offsets[i] = data.readShortInt();
        }
        start = data.length();
        written = true;
        writtenData = true;
    }

    private int getRowLength(Row row) {
        int size = 0;
        for (int i = 0; i < columnCount; i++) {
            size += data.getValueLen(row.getValue(i));
        }
        return size;
    }

    private int findInsertionPoint(long key) {
        int x = find(key);
        if (x < entryCount && keys[x] == key) {
            throw index.getDuplicateKeyException(""+key);
        }
        return x;
    }

    @Override
    int addRowTry(Row row) {
        index.getPageStore().logUndo(this, data);
        int rowLength = getRowLength(row);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        int keyOffsetPairLen = 2 + Data.getVarLongLen(row.getKey());
        if (entryCount > 0 && last - rowLength < start + keyOffsetPairLen) {
            int x = findInsertionPoint(row.getKey());
            if (entryCount > 1) {
                if (entryCount < 5) {
                    // required, otherwise the index doesn't work correctly
                    return entryCount / 2;
                }
                if (index.isSortedInsertMode()) {
                    return x < 2 ? 1 : x > entryCount - 1 ? entryCount - 1 : x;
                }
                // split near the insertion point to better fill pages
                // split in half would be:
                // return entryCount / 2;
                int third = entryCount / 3;
                return x < third ? third : x >= 2 * third ? 2 * third : x;
            }
            return x;
        }
        index.getPageStore().logUndo(this, data);
        int x;
        if (entryCount == 0) {
            x = 0;
        } else {
            if (!optimizeUpdate) {
                readAllRows();
            }
            x = findInsertionPoint(row.getKey());
        }
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        last = x == 0 ? pageSize : offsets[x - 1];
        int offset = last - rowLength;
        start += keyOffsetPairLen;
        offsets = insert(offsets, entryCount, x, offset);
        add(offsets, x + 1, entryCount + 1, -rowLength);
        keys = insert(keys, entryCount, x, row.getKey());
        rows = insert(rows, entryCount, x, row);
        entryCount++;
        index.getPageStore().update(this);
        if (optimizeUpdate) {
            if (writtenData && offset >= start) {
                byte[] d = data.getBytes();
                int dataStart = offsets[entryCount - 1] + rowLength;
                int dataEnd = offsets[x];
                System.arraycopy(d, dataStart, d, dataStart - rowLength,
                        dataEnd - dataStart + rowLength);
                data.setPos(dataEnd);
                for (int j = 0; j < columnCount; j++) {
                    data.writeValue(row.getValue(j));
                }
            }
        }
        if (offset < start) {
            writtenData = false;
            if (entryCount > 1) {
                DbException.throwInternalError("" + entryCount);
            }
            // need to write the overflow page id
            start += 4;
            int remaining = rowLength - (pageSize - start);
            // fix offset
            offset = start;
            offsets[x] = offset;
            int previous = getPos();
            int dataOffset = pageSize;
            int page = index.getPageStore().allocatePage();
            firstOverflowPageId = page;
            this.overflowRowSize = pageSize + rowLength;
            writeData();
            // free up the space used by the row
            Row r = rows[0];
            rowRef = new SoftReference<>(r);
            rows[0] = null;
            Data all = index.getPageStore().createData();
            all.checkCapacity(data.length());
            all.write(data.getBytes(), 0, data.length());
            data.truncate(index.getPageStore().getPageSize());
            do {
                int type, size, next;
                if (remaining <= pageSize - PageDataOverflow.START_LAST) {
                    type = Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST;
                    size = remaining;
                    next = 0;
                } else {
                    type = Page.TYPE_DATA_OVERFLOW;
                    size = pageSize - PageDataOverflow.START_MORE;
                    next = index.getPageStore().allocatePage();
                }
                PageDataOverflow overflow = PageDataOverflow.create(index.getPageStore(),
                        page, type, previous, next, all, dataOffset, size);
                index.getPageStore().update(overflow);
                dataOffset += size;
                remaining -= size;
                previous = page;
                page = next;
            } while (remaining > 0);
        }
        if (rowRef == null) {
            memoryChange(true, row);
        } else {
            memoryChange(true, null);
        }
        return -1;
    }

    private void removeRow(int i) {
        index.getPageStore().logUndo(this, data);
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        if (!optimizeUpdate) {
            readAllRows();
        }
        Row r = getRowAt(i);
        if (r != null) {
            memoryChange(false, r);
        }
        entryCount--;
        if (entryCount < 0) {
            DbException.throwInternalError("" + entryCount);
        }
        if (firstOverflowPageId != 0) {
            start -= 4;
            freeOverflow();
            firstOverflowPageId = 0;
            overflowRowSize = 0;
            rowRef = null;
        }
        int keyOffsetPairLen = 2 + Data.getVarLongLen(keys[i]);
        int startNext = i > 0 ? offsets[i - 1] : index.getPageStore().getPageSize();
        int rowLength = startNext - offsets[i];
        if (optimizeUpdate) {
            if (writtenData) {
                byte[] d = data.getBytes();
                int dataStart = offsets[entryCount];
                System.arraycopy(d, dataStart, d, dataStart + rowLength,
                        offsets[i] - dataStart);
                Arrays.fill(d, dataStart, dataStart + rowLength, (byte) 0);
            }
        } else {
            int clearStart = offsets[entryCount];
            Arrays.fill(data.getBytes(), clearStart, clearStart + rowLength, (byte) 0);
        }
        start -= keyOffsetPairLen;
        offsets = remove(offsets, entryCount + 1, i);
        add(offsets, i, entryCount, rowLength);
        keys = remove(keys, entryCount + 1, i);
        rows = remove(rows, entryCount + 1, i);
    }

    @Override
    Cursor find(Session session, long minKey, long maxKey, boolean multiVersion) {
        int x = find(minKey);
        return new PageDataCursor(session, this, x, maxKey, multiVersion);
    }

    /**
     * Get the row at the given index.
     *
     * @param at the index
     * @return the row
     */
    Row getRowAt(int at) {
        Row r = rows[at];
        if (r == null) {
            if (firstOverflowPageId == 0) {
                r = readRow(data, offsets[at], columnCount);
            } else {
                if (rowRef != null) {
                    r = rowRef.get();
                    if (r != null) {
                        return r;
                    }
                }
                PageStore store = index.getPageStore();
                Data buff = store.createData();
                int pageSize = store.getPageSize();
                int offset = offsets[at];
                buff.write(data.getBytes(), offset, pageSize - offset);
                int next = firstOverflowPageId;
                do {
                    PageDataOverflow page = index.getPageOverflow(next);
                    next = page.readInto(buff);
                } while (next != 0);
                overflowRowSize = pageSize + buff.length();
                r = readRow(buff, 0, columnCount);
            }
            r.setKey(keys[at]);
            if (firstOverflowPageId != 0) {
                rowRef = new SoftReference<>(r);
            } else {
                rows[at] = r;
                memoryChange(true, r);
            }
        }
        return r;
    }

    int getEntryCount() {
        return entryCount;
    }

    @Override
    PageData split(int splitPoint) {
        int newPageId = index.getPageStore().allocatePage();
        PageDataLeaf p2 = PageDataLeaf.create(index, newPageId, parentPageId);
        while (splitPoint < entryCount) {
            int split = p2.addRowTry(getRowAt(splitPoint));
            if (split != -1) {
                DbException.throwInternalError("split " + split);
            }
            removeRow(splitPoint);
        }
        return p2;
    }

    @Override
    long getLastKey() {
        // TODO re-use keys, but remove this mechanism
        if (entryCount == 0) {
            return 0;
        }
        return getRowAt(entryCount - 1).getKey();
    }

    PageDataLeaf getNextPage() {
        if (parentPageId == PageData.ROOT) {
            return null;
        }
        PageDataNode next = (PageDataNode) index.getPage(parentPageId, -1);
        return next.getNextPage(keys[entryCount - 1]);
    }

    @Override
    PageDataLeaf getFirstLeaf() {
        return this;
    }

    @Override
    protected void remapChildren(int old) {
        if (firstOverflowPageId == 0) {
            return;
        }
        PageDataOverflow overflow = index.getPageOverflow(firstOverflowPageId);
        overflow.setParentPageId(getPos());
        index.getPageStore().update(overflow);
    }

    @Override
    boolean remove(long key) {
        int i = find(key);
        if (keys == null || keys[i] != key) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                    index.getSQL() + ": " + key + " " + (keys == null ? -1 : keys[i]));
        }
        index.getPageStore().logUndo(this, data);
        if (entryCount == 1) {
            freeRecursive();
            return true;
        }
        removeRow(i);
        index.getPageStore().update(this);
        return false;
    }

    @Override
    void freeRecursive() {
        index.getPageStore().logUndo(this, data);
        index.getPageStore().free(getPos());
        freeOverflow();
    }

    private void freeOverflow() {
        if (firstOverflowPageId != 0) {
            int next = firstOverflowPageId;
            do {
                PageDataOverflow page = index.getPageOverflow(next);
                page.free();
                next = page.getNextOverflow();
            } while (next != 0);
        }
    }

    @Override
    Row getRowWithKey(long key) {
        int at = find(key);
        return getRowAt(at);
    }

    @Override
    int getRowCount() {
        return entryCount;
    }

    @Override
    void setRowCountStored(int rowCount) {
        // ignore
    }

    @Override
    long getDiskSpaceUsed() {
        return index.getPageStore().getPageSize();
    }

    @Override
    public void write() {
        writeData();
        index.getPageStore().writePage(getPos(), data);
        data.truncate(index.getPageStore().getPageSize());
    }

    private void readAllRows() {
        for (int i = 0; i < entryCount; i++) {
            getRowAt(i);
        }
    }

    private void writeHead() {
        data.reset();
        int type;
        if (firstOverflowPageId == 0) {
            type = Page.TYPE_DATA_LEAF | Page.FLAG_LAST;
        } else {
            type = Page.TYPE_DATA_LEAF;
        }
        data.writeByte((byte) type);
        data.writeShortInt(0);
        if (SysProperties.CHECK2) {
            if (data.length() != START_PARENT) {
                DbException.throwInternalError();
            }
        }
        data.writeInt(parentPageId);
        data.writeVarInt(index.getId());
        data.writeVarInt(columnCount);
        data.writeShortInt(entryCount);
    }

    private void writeData() {
        if (written) {
            return;
        }
        if (!optimizeUpdate) {
            readAllRows();
        }
        writeHead();
        if (firstOverflowPageId != 0) {
            data.writeInt(firstOverflowPageId);
            data.checkCapacity(overflowRowSize);
        }
        for (int i = 0; i < entryCount; i++) {
            data.writeVarLong(keys[i]);
            data.writeShortInt(offsets[i]);
        }
        if (!writtenData || !optimizeUpdate) {
            for (int i = 0; i < entryCount; i++) {
                data.setPos(offsets[i]);
                Row r = getRowAt(i);
                for (int j = 0; j < columnCount; j++) {
                    data.writeValue(r.getValue(j));
                }
            }
            writtenData = true;
        }
        written = true;
    }

    @Override
    public String toString() {
        return "page[" + getPos() + "] data leaf table:" +
            index.getId() + " " + index.getTable().getName() +
            " entries:" + entryCount + " parent:" + parentPageId +
            (firstOverflowPageId == 0 ? "" : " overflow:" + firstOverflowPageId) +
            " keys:" + Arrays.toString(keys) + " offsets:" + Arrays.toString(offsets);
    }

    @Override
    public void moveTo(Session session, int newPos) {
        PageStore store = index.getPageStore();
        // load the pages into the cache, to ensure old pages
        // are written
        if (parentPageId != ROOT) {
            store.getPage(parentPageId);
        }
        store.logUndo(this, data);
        PageDataLeaf p2 = PageDataLeaf.create(index, newPos, parentPageId);
        readAllRows();
        p2.keys = keys;
        p2.overflowRowSize = overflowRowSize;
        p2.firstOverflowPageId = firstOverflowPageId;
        p2.rowRef = rowRef;
        p2.rows = rows;
        if (firstOverflowPageId != 0) {
            p2.rows[0] = getRowAt(0);
        }
        p2.entryCount = entryCount;
        p2.offsets = offsets;
        p2.start = start;
        p2.remapChildren(getPos());
        p2.writeData();
        p2.data.truncate(index.getPageStore().getPageSize());
        store.update(p2);
        if (parentPageId == ROOT) {
            index.setRootPageId(session, newPos);
        } else {
            PageDataNode p = (PageDataNode) store.getPage(parentPageId);
            p.moveChild(getPos(), newPos);
        }
        store.free(getPos());
    }

    /**
     * Set the overflow page id.
     *
     * @param old the old overflow page id
     * @param overflow the new overflow page id
     */
    void setOverflow(int old, int overflow) {
        if (SysProperties.CHECK && old != firstOverflowPageId) {
            DbException.throwInternalError("move " + this + " " + firstOverflowPageId);
        }
        index.getPageStore().logUndo(this, data);
        firstOverflowPageId = overflow;
        if (written) {
            changeCount = index.getPageStore().getChangeCount();
            writeHead();
            data.writeInt(firstOverflowPageId);
        }
        index.getPageStore().update(this);
    }

    private void memoryChange(boolean add, Row r) {
        int diff = r == null ? 0 : 4 + 8 + Constants.MEMORY_POINTER + r.getMemory();
        memoryData += add ? diff : -diff;
        index.memoryChange((Constants.MEMORY_PAGE_DATA +
                memoryData + index.getPageStore().getPageSize()) >> 2);
    }

    @Override
    public boolean isStream() {
        return firstOverflowPageId > 0;
    }

    /**
     * Read a row from the data page at the given position.
     *
     * @param data the data page
     * @param pos the position to read from
     * @param columnCount the number of columns
     * @return the row
     */
    private Row readRow(Data data, int pos, int columnCount) {
        Value[] values = new Value[columnCount];
        synchronized (data) {
            data.setPos(pos);
            for (int i = 0; i < columnCount; i++) {
                values[i] = data.readValue();
            }
        }
        return index.getDatabase().createRow(values, Row.MEMORY_CALCULATE);
    }

}
