/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.HashSet;
import org.h2.compress.Compressor;
import org.h2.mvstore.type.DataType;
import org.h2.util.Utils;

/**
 * A page (a node or a leaf).
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of
 * the child at the same index.
 * <p>
 * File format:
 * page length (including length): int
 * check value: short
 * map id: varInt
 * number of keys: varInt
 * type: byte (0: leaf, 1: node; +2: compressed)
 * compressed: bytes saved (varInt)
 * keys
 * leaf: values (one for each key)
 * node: children (1 more than keys)
 */
public class Page {

    /**
     * An empty object array.
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    private static final int IN_MEMORY = Integer.MIN_VALUE;

    private final MVMap<?, ?> map;
    private long version;
    private long pos;

    /**
     * The total entry count of this page and all children.
     */
    private long totalCount;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used in persistent case, IN_MEMORY marker value otherwise.
     */
    private int memory;

    /**
     * The keys.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private Object[] keys;

    /**
     * The values.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private Object[] values;

    /**
     * The child page references.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private PageReference[] children;

    /**
     * Whether the page is an in-memory (not stored, or not yet stored) page,
     * and it is removed. This is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     */
    private volatile boolean removedInMemory;

    Page(MVMap<?, ?> map, long version) {
        this.map = map;
        this.version = version;
    }

    /**
     * Create a new, empty page.
     *
     * @param map the map
     * @param version the version
     * @return the new page
     */
    static Page createEmpty(MVMap<?, ?> map, long version) {
        return create(map, version,
                EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY,
                null,
                0, DataUtils.PAGE_MEMORY);
    }

    /**
     * Create a new page. The arrays are not cloned.
     *
     * @param map the map
     * @param version the version
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static Page create(MVMap<?, ?> map, long version,
            Object[] keys, Object[] values, PageReference[] children,
            long totalCount, int memory) {
        Page p = new Page(map, version);
        // the position is 0
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.totalCount = totalCount;
        MVStore store = map.store;
        if(store.getFileStore() == null) {
            p.memory = IN_MEMORY;
        } else if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        if(store.getFileStore() != null) {
            store.registerUnsavedPage(p.memory);
        }
        return p;
    }

    /**
     * Create a copy of a page.
     *
     * @param map the map
     * @param version the version
     * @param source the source page
     * @return the page
     */
    public static Page create(MVMap<?, ?> map, long version, Page source) {
        return create(map, version, source.keys, source.values, source.children,
                source.totalCount, source.memory);
    }

    /**
     * Read a page.
     *
     * @param fileStore the file store
     * @param pos the position
     * @param map the map
     * @param filePos the position in the file
     * @param maxPos the maximum position (the end of the chunk)
     * @return the page
     */
    static Page read(FileStore fileStore, long pos, MVMap<?, ?> map,
            long filePos, long maxPos) {
        ByteBuffer buff;
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ",
                    length, filePos, maxPos);
        }
        buff = fileStore.readFully(filePos, length);
        Page p = new Page(map, 0);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    /**
     * Get the key at the given index.
     *
     * @param index the index
     * @return the key
     */
    public Object getKey(int index) {
        return keys[index];
    }

    /**
     * Get the child page at the given index.
     *
     * @param index the index
     * @return the child page
     */
    public Page getChildPage(int index) {
        PageReference ref = children[index];
        return ref.page != null ? ref.page : map.readPage(ref.pos);
    }

    /**
     * Get the position of the child.
     *
     * @param index the index
     * @return the position
     */
    public long getChildPagePos(int index) {
        return children[index].pos;
    }

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public Object getValue(int index) {
        return values[index];
    }

    /**
     * Get the number of keys in this page.
     *
     * @return the number of keys
     */
    public int getKeyCount() {
        return keys.length;
    }

    /**
     * Check whether this is a leaf page.
     *
     * @return true if it is a leaf
     */
    public boolean isLeaf() {
        return children == null;
    }

    /**
     * Get the position of the page
     *
     * @return the position
     */
    public long getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("version: ").append(Long.toHexString(version)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append('\n');
        if (pos != 0) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append('\n');
        }
        for (int i = 0; i <= keys.length; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            if (children != null) {
                buff.append('[').append(Long.toHexString(children[i].pos)).append("] ");
            }
            if (i < keys.length) {
                buff.append(keys[i]);
                if (values != null) {
                    buff.append(':');
                    buff.append(values[i]);
                }
            }
        }
        return buff.toString();
    }

    /**
     * Create a copy of this page.
     *
     * @param version the new version
     * @return a page with the given version
     */
    public Page copy(long version) {
        Page newPage = create(map, version,
                keys, values,
                children, totalCount,
                memory);
        // mark the old as deleted
        removePage();
        newPage.cachedCompare = cachedCompare;
        return newPage;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the value or null
     */
    public int binarySearch(Object key) {
        int low = 0, high = keys.length - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = cachedCompare - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }
        Object[] k = keys;
        while (low <= high) {
            int compare = map.compare(key, k[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                cachedCompare = x + 1;
                return x;
            }
            x = (low + high) >>> 1;
        }
        cachedCompare = low;
        return -(low + 1);

        // regular binary search (without caching)
        // int low = 0, high = keys.length - 1;
        // while (low <= high) {
        //     int x = (low + high) >>> 1;
        //     int compare = map.compare(key, keys[x]);
        //     if (compare > 0) {
        //         low = x + 1;
        //     } else if (compare < 0) {
        //         high = x - 1;
        //     } else {
        //         return x;
        //     }
        // }
        // return -(low + 1);
    }

    /**
     * Split the page. This modifies the current page.
     *
     * @param at the split index
     * @return the page with the entries after the split index
     */
    Page split(int at) {
        Page page = isLeaf() ? splitLeaf(at) : splitNode(at);
        if(isPersistent()) {
            recalculateMemory();
        }
        return page;
    }

    private Page splitLeaf(int at) {
        int b = keys.length - at;
        Object[] aKeys = new Object[at];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, at);
        System.arraycopy(keys, at, bKeys, 0, b);
        keys = aKeys;
        Object[] aValues = new Object[at];
        Object[] bValues = new Object[b];
        bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, at);
        System.arraycopy(values, at, bValues, 0, b);
        values = aValues;
        totalCount = at;
        return create(map, version,
                bKeys, bValues,
                null,
                b, 0);
    }

    private Page splitNode(int at) {
        int b = keys.length - at;

        Object[] aKeys = new Object[at];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, at);
        System.arraycopy(keys, at + 1, bKeys, 0, b - 1);
        keys = aKeys;

        PageReference[] aChildren = new PageReference[at + 1];
        PageReference[] bChildren = new PageReference[b];
        System.arraycopy(children, 0, aChildren, 0, at + 1);
        System.arraycopy(children, at + 1, bChildren, 0, b);
        children = aChildren;

        long t = 0;
        for (PageReference x : aChildren) {
            t += x.count;
        }
        totalCount = t;
        t = 0;
        for (PageReference x : bChildren) {
            t += x.count;
        }
        return create(map, version,
                bKeys, null,
                bChildren,
                t, 0);
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     *
     * @return the number of key-value pairs
     */
    public long getTotalCount() {
        if (MVStore.ASSERT) {
            long check = 0;
            if (isLeaf()) {
                check = keys.length;
            } else {
                for (PageReference x : children) {
                    check += x.count;
                }
            }
            if (check != totalCount) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Expected: {0} got: {1}", check, totalCount);
            }
        }
        return totalCount;
    }

    /**
     * Get the descendant counts for the given child.
     *
     * @param index the child index
     * @return the descendant count
     */
    long getCounts(int index) {
        return children[index].count;
    }

    /**
     * Replace the child page.
     *
     * @param index the index
     * @param c the new child page
     */
    public void setChild(int index, Page c) {
        if (c == null) {
            long oldCount = children[index].count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(null, 0, 0);
            children[index] = ref;
            totalCount -= oldCount;
        } else if (c != children[index].page ||
                c.getPos() != children[index].pos) {
            long oldCount = children[index].count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(c, c.pos, c.totalCount);
            children[index] = ref;
            totalCount += c.totalCount - oldCount;
        }
    }

    /**
     * Replace the key at an index in this page.
     *
     * @param index the index
     * @param key the new key
     */
    public void setKey(int index, Object key) {
        // this is slightly slower:
        // keys = Arrays.copyOf(keys, keys.length);
        keys = keys.clone();
        if(isPersistent()) {
            Object old = keys[index];
            DataType keyType = map.getKeyType();
            int mem = keyType.getMemory(key);
            if (old != null) {
                mem -= keyType.getMemory(old);
            }
            addMemory(mem);
        }
        keys[index] = key;
    }

    /**
     * Replace the value at an index in this page.
     *
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public Object setValue(int index, Object value) {
        Object old = values[index];
        // this is slightly slower:
        // values = Arrays.copyOf(values, values.length);
        values = values.clone();
        DataType valueType = map.getValueType();
        if(isPersistent()) {
            addMemory(valueType.getMemory(value) -
                    valueType.getMemory(old));
        }
        values[index] = value;
        return old;
    }

    /**
     * Remove this page and all child pages.
     */
    void removeAllRecursive() {
        if (children != null) {
            for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                PageReference ref = children[i];
                if (ref.page != null) {
                    ref.page.removeAllRecursive();
                } else {
                    long c = children[i].pos;
                    int type = DataUtils.getPageType(c);
                    if (type == DataUtils.PAGE_TYPE_LEAF) {
                        int mem = DataUtils.getPageMaxLength(c);
                        map.removePage(c, mem);
                    } else {
                        map.readPage(c).removeAllRecursive();
                    }
                }
            }
        }
        removePage();
    }

    /**
     * Insert a key-value pair into this leaf.
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public void insertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        keys = newKeys;
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        values = newValues;
        keys[index] = key;
        values[index] = value;
        totalCount++;
        if(isPersistent()) {
            addMemory(map.getKeyType().getMemory(key) +
                    map.getValueType().getMemory(value));
        }
    }

    /**
     * Insert a child page into this node.
     *
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public void insertNode(int index, Object key, Page childPage) {

        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = new PageReference(
                childPage, childPage.getPos(), childPage.totalCount);
        children = newChildren;

        totalCount += childPage.totalCount;
        if(isPersistent()) {
            addMemory(map.getKeyType().getMemory(key) +
                    DataUtils.PAGE_MEMORY_CHILD);
        }
    }

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        int keyLength = keys.length;
        int keyIndex = index >= keyLength ? index - 1 : index;
        if(isPersistent()) {
            Object old = keys[keyIndex];
            addMemory(-map.getKeyType().getMemory(old));
        }
        Object[] newKeys = new Object[keyLength - 1];
        DataUtils.copyExcept(keys, newKeys, keyLength, keyIndex);
        keys = newKeys;

        if (values != null) {
            if(isPersistent()) {
                Object old = values[index];
                addMemory(-map.getValueType().getMemory(old));
            }
            Object[] newValues = new Object[keyLength - 1];
            DataUtils.copyExcept(values, newValues, keyLength, index);
            values = newValues;
            totalCount--;
        }
        if (children != null) {
            if(isPersistent()) {
                addMemory(-DataUtils.PAGE_MEMORY_CHILD);
            }
            long countOffset = children[index].count;

            int childCount = children.length;
            PageReference[] newChildren = new PageReference[childCount - 1];
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;

            totalCount -= countOffset;
        }
    }

    /**
     * Read the page from the buffer.
     *
     * @param buff the buffer
     * @param chunkId the chunk id
     * @param offset the offset within the chunk
     * @param maxLength the maximum length
     */
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}",
                    chunkId, maxLength, pageLength);
        }
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int mapId = DataUtils.readVarInt(buff);
        if (mapId != map.getId()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}",
                    chunkId, map.getId(), mapId);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}",
                    chunkId, checkTest, check);
        }
        int len = DataUtils.readVarInt(buff);
        keys = new Object[len];
        int type = buff.get();
        boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
        if (node) {
            children = new PageReference[len + 1];
            long[] p = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= len; i++) {
                long s = DataUtils.readVarLong(buff);
                total += s;
                children[i] = new PageReference(null, p[i], s);
            }
            totalCount = total;
        }
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) ==
                    DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getStore().getCompressorHigh();
            } else {
                compressor = map.getStore().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = Utils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, buff.array(),
                    buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, len, true);
        if (!node) {
            values = new Object[len];
            map.getValueType().read(buff, values, len, false);
            totalCount = len;
        }
        recalculateMemory();
    }

    /**
     * Store the page and update the position.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     * @return the position of the buffer just after the type
     */
    private int write(Chunk chunk, WriteBuffer buff) {
        int start = buff.position();
        int len = keys.length;
        int type = children != null ? DataUtils.PAGE_TYPE_NODE
                : DataUtils.PAGE_TYPE_LEAF;
        buff.putInt(0).
            putShort((byte) 0).
            putVarInt(map.getId()).
            putVarInt(len);
        int typePos = buff.position();
        buff.put((byte) type);
        if (type == DataUtils.PAGE_TYPE_NODE) {
            writeChildren(buff);
            for (int i = 0; i <= len; i++) {
                buff.putVarLong(children[i].count);
            }
        }
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, len, true);
        if (type == DataUtils.PAGE_TYPE_LEAF) {
            map.getValueType().write(buff, values, len, false);
        }
        MVStore store = map.getStore();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = store.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = map.getStore().getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = map.getStore().getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).
                        put((byte) (type + compressType));
                    buff.position(compressStart).
                        putVarInt(expLen - compLen).
                        put(comp, 0, compLen);
                }
            }
        }
        int pageLength = buff.position() - start;
        int chunkId = chunk.id;
        int check = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(start, pageLength).
            putShort(start + 4, (short) check);
        if (pos != 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = DataUtils.getPagePos(chunkId, start, pageLength, type);
        store.cachePage(pos, this, getMemory());
        if (type == DataUtils.PAGE_TYPE_NODE) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            store.cachePage(pos, this, getMemory());
        }
        long max = DataUtils.getPageMaxLength(pos);
        chunk.maxLen += max;
        chunk.maxLenLive += max;
        chunk.pageCount++;
        chunk.pageCountLive++;
        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            map.removePage(pos, memory);
        }
        return typePos + 1;
    }

    private void writeChildren(WriteBuffer buff) {
        int len = keys.length;
        for (int i = 0; i <= len; i++) {
            buff.putLong(children[i].pos);
        }
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     */
    void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        int patch = write(chunk, buff);
        if (!isLeaf()) {
            int len = children.length;
            for (int i = 0; i < len; i++) {
                Page p = children[i].page;
                if (p != null) {
                    p.writeUnsavedRecursive(chunk, buff);
                    children[i] = new PageReference(p, p.getPos(), p.totalCount);
                }
            }
            int old = buff.position();
            buff.position(patch);
            writeChildren(buff);
            buff.position(old);
        }
    }

    /**
     * Unlink the children recursively after all data is written.
     */
    void writeEnd() {
        if (isLeaf()) {
            return;
        }
        int len = children.length;
        for (int i = 0; i < len; i++) {
            PageReference ref = children[i];
            if (ref.page != null) {
                if (ref.page.getPos() == 0) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_INTERNAL, "Page not written");
                }
                ref.page.writeEnd();
                children[i] = new PageReference(null, ref.pos, ref.count);
            }
        }
    }

    long getVersion() {
        return version;
    }

    public int getRawChildPageCount() {
        return children.length;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof Page) {
            if (pos != 0 && ((Page) other).pos == pos) {
                return true;
            }
            return this == other;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return pos != 0 ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    private boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    public int getMemory() {
        if (isPersistent()) {
            if (MVStore.ASSERT) {
                int mem = memory;
                recalculateMemory();
                if (mem != memory) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_INTERNAL, "Memory calculation error");
                }
            }
            return memory;
        }
        return getKeyCount();
    }

    private void addMemory(int mem) {
        memory += mem;
    }

    private void recalculateMemory() {
        int mem = DataUtils.PAGE_MEMORY;
        DataType keyType = map.getKeyType();
        for (Object key : keys) {
            mem += keyType.getMemory(key);
        }
        if (this.isLeaf()) {
            DataType valueType = map.getValueType();
            for (int i = 0; i < keys.length; i++) {
                mem += valueType.getMemory(values[i]);
            }
        } else {
            mem += this.getRawChildPageCount() * DataUtils.PAGE_MEMORY_CHILD;
        }
        addMemory(mem - memory);
    }

    void setVersion(long version) {
        this.version = version;
    }

    /**
     * Remove the page.
     */
    public void removePage() {
        if(isPersistent()) {
            long p = pos;
            if (p == 0) {
                removedInMemory = true;
            }
            map.removePage(p, memory);
        }
    }

    /**
     * A pointer to a page, either in-memory or using a page position.
     */
    public static class PageReference {

        /**
         * The position, if known, or 0.
         */
        final long pos;

        /**
         * The page, if in memory, or null.
         */
        final Page page;

        /**
         * The descendant count for this child page.
         */
        final long count;

        public PageReference(Page page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

    }

    /**
     * Contains information about which other pages are referenced (directly or
     * indirectly) by the given page. This is a subset of the page data, for
     * pages of type node. This information is used for garbage collection (to
     * quickly find out which chunks are still in use).
     */
    public static class PageChildren {

        /**
         * An empty array of type long.
         */
        public static final long[] EMPTY_ARRAY = new long[0];

        /**
         * The position of the page.
         */
        final long pos;

        /**
         * The page positions of (direct or indirect) children. Depending on the
         * use case, this can be the complete list, or only a subset of all
         * children, for example only only one reference to a child in another
         * chunk.
         */
        long[] children;

        /**
         * Whether this object only contains the list of chunks.
         */
        boolean chunkList;

        private PageChildren(long pos, long[] children) {
            this.pos = pos;
            this.children = children;
        }

        PageChildren(Page p) {
            this.pos = p.getPos();
            int count = p.getRawChildPageCount();
            this.children = new long[count];
            for (int i = 0; i < count; i++) {
                children[i] = p.getChildPagePos(i);
            }
        }

        int getMemory() {
            return 64 + 8 * children.length;
        }

        /**
         * Read an inner node page from the buffer, but ignore the keys and
         * values.
         *
         * @param fileStore the file store
         * @param pos the position
         * @param mapId the map id
         * @param filePos the position in the file
         * @param maxPos the maximum position (the end of the chunk)
         * @return the page children object
         */
        static PageChildren read(FileStore fileStore, long pos, int mapId,
                long filePos, long maxPos) {
            ByteBuffer buff;
            int maxLength = DataUtils.getPageMaxLength(pos);
            if (maxLength == DataUtils.PAGE_LARGE) {
                buff = fileStore.readFully(filePos, 128);
                maxLength = buff.getInt();
                // read the first bytes again
            }
            maxLength = (int) Math.min(maxPos - filePos, maxLength);
            int length = maxLength;
            if (length < 0) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Illegal page length {0} reading at {1}; max pos {2} ",
                        length, filePos, maxPos);
            }
            buff = fileStore.readFully(filePos, length);
            int chunkId = DataUtils.getPageChunkId(pos);
            int offset = DataUtils.getPageOffset(pos);
            int start = buff.position();
            int pageLength = buff.getInt();
            if (pageLength > maxLength) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "File corrupted in chunk {0}, expected page length =< {1}, got {2}",
                        chunkId, maxLength, pageLength);
            }
            buff.limit(start + pageLength);
            short check = buff.getShort();
            int m = DataUtils.readVarInt(buff);
            if (m != mapId) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "File corrupted in chunk {0}, expected map id {1}, got {2}",
                        chunkId, mapId, m);
            }
            int checkTest = DataUtils.getCheckValue(chunkId)
                    ^ DataUtils.getCheckValue(offset)
                    ^ DataUtils.getCheckValue(pageLength);
            if (check != (short) checkTest) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "File corrupted in chunk {0}, expected check value {1}, got {2}",
                        chunkId, checkTest, check);
            }
            int len = DataUtils.readVarInt(buff);
            int type = buff.get();
            boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
            if (!node) {
                return null;
            }
            long[] children = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                children[i] = buff.getLong();
            }
            return new PageChildren(pos, children);
        }

        /**
         * Only keep one reference to the same chunk. Only leaf references are
         * removed (references to inner nodes are not removed, as they could
         * indirectly point to other chunks).
         */
        void removeDuplicateChunkReferences() {
            HashSet<Integer> chunks = new HashSet<>();
            // we don't need references to leaves in the same chunk
            chunks.add(DataUtils.getPageChunkId(pos));
            for (int i = 0; i < children.length; i++) {
                long p = children[i];
                int chunkId = DataUtils.getPageChunkId(p);
                boolean wasNew = chunks.add(chunkId);
                if (DataUtils.getPageType(p) == DataUtils.PAGE_TYPE_NODE) {
                    continue;
                }
                if (wasNew) {
                    continue;
                }
                removeChild(i--);
            }
        }

        private void removeChild(int index) {
            if (index == 0 && children.length == 1) {
                children = EMPTY_ARRAY;
                return;
            }
            long[] c2 = new long[children.length - 1];
            DataUtils.copyExcept(children, c2, children.length, index);
            children = c2;
        }

    }

}
