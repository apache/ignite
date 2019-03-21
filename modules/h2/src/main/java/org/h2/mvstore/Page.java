/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.engine.Constants.MEMORY_ARRAY;
import static org.h2.engine.Constants.MEMORY_OBJECT;
import static org.h2.engine.Constants.MEMORY_POINTER;
import static org.h2.mvstore.DataUtils.PAGE_TYPE_LEAF;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.compress.Compressor;
import org.h2.message.DbException;
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
public abstract class Page implements Cloneable
{
    /**
     * Map this page belongs to
     */
    public final MVMap<?, ?> map;

    /**
     * Position of this page's saved image within a Chunk or 0 if this page has not been saved yet.
     */
    private long pos;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used in persistent case, IN_MEMORY marker value otherwise.
     */
    private int memory;

    /**
     * Amount of used disk space by this page only in persistent case.
     */
    private int diskSpaceUsed;

    /**
     * The keys.
     */
    private Object[] keys;

    /**
     * Whether the page is an in-memory (not stored, or not yet stored) page,
     * and it is removed. This is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     */
    private volatile boolean removedInMemory;

    /**
     * The estimated number of bytes used per child entry.
     */
    static final int PAGE_MEMORY_CHILD = MEMORY_POINTER + 16; //  16 = two longs

    /**
     * The estimated number of bytes used per base page.
     */
    private static final int PAGE_MEMORY =
            MEMORY_OBJECT +           // this
            2 * MEMORY_POINTER +      // map, keys
            MEMORY_ARRAY +            // Object[] keys
            17;                       // pos, cachedCompare, memory, removedInMemory
    /**
     * The estimated number of bytes used per empty internal page object.
     */
    static final int PAGE_NODE_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // children
            MEMORY_ARRAY +            // Object[] children
            8;                        // totalCount

    /**
     * The estimated number of bytes used per empty leaf page.
     */
    static final int PAGE_LEAF_MEMORY =
            PAGE_MEMORY +             // super
            MEMORY_POINTER +          // values
            MEMORY_ARRAY;             // Object[] values

    /**
     * An empty object array.
     */
    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /**
     * Marker value for memory field, meaning that memory accounting is replaced by key count.
     */
    private static final int IN_MEMORY = Integer.MIN_VALUE;

    private static final PageReference[] SINGLE_EMPTY = { PageReference.EMPTY };


    Page(MVMap<?, ?> map) {
        this.map = map;
    }

    Page(MVMap<?, ?> map, Page source) {
        this(map, source.keys);
        memory = source.memory;
    }

    Page(MVMap<?, ?> map, Object[] keys) {
        this.map = map;
        this.keys = keys;
    }

    /**
     * Create a new, empty leaf page.
     *
     * @param map the map
     * @return the new page
     */
    static Page createEmptyLeaf(MVMap<?, ?> map) {
        return createLeaf(map, EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY, PAGE_LEAF_MEMORY);
    }

    /**
     * Create a new, empty internal node page.
     *
     * @param map the map
     * @return the new page
     */
    static Page createEmptyNode(MVMap<?, ?> map) {
        return createNode(map, EMPTY_OBJECT_ARRAY, SINGLE_EMPTY, 0,
                            PAGE_NODE_MEMORY + MEMORY_POINTER + PAGE_MEMORY_CHILD); // there is always one child
    }

    /**
     * Create a new non-leaf page. The arrays are not cloned.
     *
     * @param map the map
     * @param keys the keys
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static Page createNode(MVMap<?, ?> map, Object[] keys, PageReference[] children,
                                    long totalCount, int memory) {
        assert keys != null;
        Page page = new NonLeaf(map, keys, children, totalCount);
        page.initMemoryAccount(memory);
        return page;
    }

    /**
     * Create a new leaf page. The arrays are not cloned.
     *
     * @param map the map
     * @param keys the keys
     * @param values the values
     * @param memory the memory used in bytes
     * @return the page
     */
    public static Page createLeaf(MVMap<?, ?> map, Object[] keys, Object[] values, int memory) {
        assert keys != null;
        Page page = new Leaf(map, keys, values);
        page.initMemoryAccount(memory);
        return page;
    }

    private void initMemoryAccount(int memoryCount) {
        if(map.store.getFileStore() == null) {
            memory = IN_MEMORY;
        } else if (memoryCount == 0) {
            recalculateMemory();
        } else {
            addMemory(memoryCount);
            assert memoryCount == getMemory();
        }
    }

    /**
     * Get the value for the given key, or null if not found.
     * Search is done in the tree rooted at given page.
     *
     * @param key the key
     * @param p the root page
     * @return the value, or null if not found
     */
    static Object get(Page p, Object key) {
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                return index >= 0 ? p.getValue(index) : null;
            } else if (index++ < 0) {
                index = -index;
            }
            p = p.getChildPage(index);
        }
    }

    /**
     * Read a page.
     *
     * @param buff ByteBuffer containing serialized page info
     * @param pos the position
     * @param map the map
     * @return the page
     */
    static Page read(ByteBuffer buff, long pos, MVMap<?, ?> map) {
        boolean leaf = (DataUtils.getPageType(pos) & 1) == PAGE_TYPE_LEAF;
        Page p = leaf ? new Leaf(map) : new NonLeaf(map);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        p.read(buff, chunkId);
        return p;
    }

    /**
     * Read an inner node page from the buffer, but ignore the keys and
     * values.
     *
     * @param buff ByteBuffer containing serialized page info
     * @param pos the position
     * @param collector to report child pages positions to
     * @param executorService to use far parallel processing
     * @param executingThreadCounter for parallel processing
     */
    static void readChildrenPositions(ByteBuffer buff, long pos,
                                        final MVStore.ChunkIdsCollector collector,
                                        final ThreadPoolExecutor executorService,
                                        final AtomicInteger executingThreadCounter) {
        int len = DataUtils.readVarInt(buff);
        int type = buff.get();
        if ((type & 1) != DataUtils.PAGE_TYPE_NODE) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Position {0} expected to be a non-leaf", pos);
        }
        /*
         * The logic here is a little awkward. We want to (a) execute reads in parallel, but (b)
         * limit the number of threads we create. This is complicated by (a) the algorithm is
         * recursive and needs to wait for children before returning up the call-stack, (b) checking
         * the size of the thread-pool is not reliable.
         */
        final List<Future<?>> futures = new ArrayList<>(len + 1);
        for (int i = 0; i <= len; i++) {
            final long childPagePos = buff.getLong();
            for (;;) {
                int counter = executingThreadCounter.get();
                if (counter >= executorService.getMaximumPoolSize()) {
                    collector.visit(childPagePos, executorService, executingThreadCounter);
                    break;
                } else {
                    if (executingThreadCounter.compareAndSet(counter, counter + 1)) {
                        Future<?> f = executorService.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    collector.visit(childPagePos, executorService, executingThreadCounter);
                                } finally {
                                    executingThreadCounter.decrementAndGet();
                                }
                            }
                        });
                        futures.add(f);
                        break;
                    }
                }
            }
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (ExecutionException ex) {
                throw DbException.convert(ex);
            }
        }
    }

    /**
     * Get the id of the page's owner map
     * @return id
     */
    public final int getMapId() {
        return map.getId();
    }

    /**
     * Create a copy of this page with potentially different owning map.
     * This is used exclusively during bulk map copying.
     * Child page references for nodes are cleared (re-pointed to an empty page)
     * to be filled-in later to copying procedure. This way it can be saved
     * mid-process without tree integrity violation
     *
     * @param map new map to own resulting page
     * @return the page
     */
    abstract Page copy(MVMap<?, ?> map);

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
    public abstract Page getChildPage(int index);

    /**
     * Get the child page at the given index only if is
     * already loaded. Does not make any attempt to load
     * the page or retrieve it from the cache.
     *
     * @param index the index
     * @return the child page, null if it is not loaded
     */
    public abstract Page getChildPageIfLoaded(int index);

    /**
     * Get the position of the child.
     *
     * @param index the index
     * @return the position
     */
    public abstract long getChildPagePos(int index);

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public abstract Object getValue(int index);

    /**
     * Get the number of keys in this page.
     *
     * @return the number of keys
     */
    public final int getKeyCount() {
        return keys.length;
    }

    /**
     * Check whether this is a leaf page.
     *
     * @return true if it is a leaf
     */
    public final boolean isLeaf() {
        return getNodeType() == PAGE_TYPE_LEAF;
    }

    public abstract int getNodeType();

    /**
     * Get the position of the page
     *
     * @return the position
     */
    public final long getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        dump(buff);
        return buff.toString();
    }

    /**
     * Dump debug data for this page.
     *
     * @param buff append buffer
     */
    protected void dump(StringBuilder buff) {
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(pos)).append('\n');
        if (isSaved()) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append('\n');
        }
    }

    /**
     * Create a copy of this page.
     *
     * @return a mutable copy of this page
     */
    public final Page copy() {
        return copy(false);
    }

    /**
     * Create a copy of this page.
     *
     * @param countRemoval When {@code true} the current page is removed,
     *                     when {@code false} just copy the page.
     * @return a mutable copy of this page
     */
    public final Page copy(boolean countRemoval) {
        Page newPage = clone();
        newPage.pos = 0;
        // mark the old as deleted
        if(countRemoval) {
            removePage();
            if(isPersistent()) {
                map.store.registerUnsavedPage(newPage.getMemory());
            }
        }
        return newPage;
    }

    @Override
    protected final Page clone() {
        Page clone;
        try {
            clone = (Page) super.clone();
        } catch (CloneNotSupportedException impossible) {
            throw new RuntimeException(impossible);
        }
        return clone;
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
    int binarySearch(Object key) {
        int low = 0, high = getKeyCount() - 1;
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
    }

    /**
     * Split the page. This modifies the current page.
     *
     * @param at the split index
     * @return the page with the entries after the split index
     */
    abstract Page split(int at);

    /**
     * Split the current keys array into two arrays.
     *
     * @param aCount size of the first array.
     * @param bCount size of the second array/
     * @return the second array.
     */
    final Object[] splitKeys(int aCount, int bCount) {
        assert aCount + bCount <= getKeyCount();
        Object[] aKeys = createKeyStorage(aCount);
        Object[] bKeys = createKeyStorage(bCount);
        System.arraycopy(keys, 0, aKeys, 0, aCount);
        System.arraycopy(keys, getKeyCount() - bCount, bKeys, 0, bCount);
        keys = aKeys;
        return bKeys;
    }

    /**
     * Append additional key/value mappings to this Page.
     * New mappings suppose to be in correct key order.
     *
     * @param extraKeyCount number of mappings to be added
     * @param extraKeys to be added
     * @param extraValues to be added
     */
    abstract void expand(int extraKeyCount, Object[] extraKeys, Object[] extraValues);

    /**
     * Expand the keys array.
     *
     * @param extraKeyCount number of extra key entries to create
     * @param extraKeys extra key values
     */
    final void expandKeys(int extraKeyCount, Object[] extraKeys) {
        int keyCount = getKeyCount();
        Object[] newKeys = createKeyStorage(keyCount + extraKeyCount);
        System.arraycopy(keys, 0, newKeys, 0, keyCount);
        System.arraycopy(extraKeys, 0, newKeys, keyCount, extraKeyCount);
        keys = newKeys;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     *
     * @return the number of key-value pairs
     */
    public abstract long getTotalCount();

    /**
     * Get the number of key-value pairs for a given child.
     *
     * @param index the child index
     * @return the descendant count
     */
    abstract long getCounts(int index);

    /**
     * Replace the child page.
     *
     * @param index the index
     * @param c the new child page
     */
    public abstract void setChild(int index, Page c);

    /**
     * Replace the key at an index in this page.
     *
     * @param index the index
     * @param key the new key
     */
    public final void setKey(int index, Object key) {
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
    public abstract Object setValue(int index, Object value);

    /**
     * Insert a key-value pair into this leaf.
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public abstract void insertLeaf(int index, Object key, Object value);

    /**
     * Insert a child page into this node.
     *
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public abstract void insertNode(int index, Object key, Page childPage);

    /**
     * Insert a key into the key array
     *
     * @param index index to insert at
     * @param key the key value
     */
    final void insertKey(int index, Object key) {
        int keyCount = getKeyCount();
        assert index <= keyCount : index + " > " + keyCount;
        Object[] newKeys = createKeyStorage(keyCount + 1);
        DataUtils.copyWithGap(keys, newKeys, keyCount, index);
        keys = newKeys;

        keys[index] = key;

        if (isPersistent()) {
            addMemory(MEMORY_POINTER + map.getKeyType().getMemory(key));
        }
    }

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        int keyCount = getKeyCount();
        DataType keyType = map.getKeyType();
        if (index == keyCount) {
            --index;
        }
        if(isPersistent()) {
            Object old = getKey(index);
            addMemory(-MEMORY_POINTER - keyType.getMemory(old));
        }
        Object[] newKeys = createKeyStorage(keyCount - 1);
        DataUtils.copyExcept(keys, newKeys, keyCount, index);
        keys = newKeys;
    }

    /**
     * Read the page from the buffer.
     *
     * @param buff the buffer
     * @param chunkId the chunk id
     */
    private void read(ByteBuffer buff, int chunkId) {
        int pageLength = buff.remaining() + 4;  // size of int, since we've read page length already
        int len = DataUtils.readVarInt(buff);
        keys = createKeyStorage(len);
        int type = buff.get();
        if(isLeaf() != ((type & 1) == PAGE_TYPE_LEAF)) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected node type {1}, got {2}",
                    chunkId, isLeaf() ? "0" : "1" , type);
        }
        if (!isLeaf()) {
            readPayLoad(buff);
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
            int compLen = buff.remaining();
            byte[] comp = Utils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, buff.array(),
                    buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, len, true);
        if (isLeaf()) {
            readPayLoad(buff);
        }
        diskSpaceUsed = pageLength;
        recalculateMemory();
    }

    /**
     * Read the page payload from the buffer.
     *
     * @param buff the buffer
     */
    protected abstract void readPayLoad(ByteBuffer buff);

    public final boolean isSaved() {
        return DataUtils.isPageSaved(pos);
    }

    /**
     * Store the page and update the position.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     * @return the position of the buffer just after the type
     */
    protected final int write(Chunk chunk, WriteBuffer buff) {
        int start = buff.position();
        int len = getKeyCount();
        int type = isLeaf() ? PAGE_TYPE_LEAF : DataUtils.PAGE_TYPE_NODE;
        buff.putInt(0).
            putShort((byte) 0).
            putVarInt(map.getId()).
            putVarInt(len);
        int typePos = buff.position();
        buff.put((byte) type);
        writeChildren(buff, true);
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, len, true);
        writeValues(buff);
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
        if (isSaved()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = DataUtils.getPagePos(chunkId, start, pageLength, type);
        store.cachePage(this);
        if (type == DataUtils.PAGE_TYPE_NODE) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            store.cachePage(this);
        }
        int max = DataUtils.getPageMaxLength(pos);
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
        diskSpaceUsed = max != DataUtils.PAGE_LARGE ? max : pageLength;
        return typePos + 1;
    }

    /**
     * Write values that the buffer contains to the buff.
     *
     * @param buff the target buffer
     */
    protected abstract void writeValues(WriteBuffer buff);

    /**
     * Write page children to the buff.
     *
     * @param buff the target buffer
     * @param withCounts true if the descendant counts should be written
     */
    protected abstract void writeChildren(WriteBuffer buff, boolean withCounts);

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     */
    abstract void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff);

    /**
     * Unlink the children recursively after all data is written.
     */
    abstract void writeEnd();

    public abstract int getRawChildPageCount();

    @Override
    public final boolean equals(Object other) {
        return other == this || other instanceof Page && isSaved() && ((Page) other).pos == pos;
    }

    @Override
    public final int hashCode() {
        return isSaved() ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    protected final boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    public final int getMemory() {
        if (isPersistent()) {
//            assert memory == calculateMemory() :
//                    "Memory calculation error " + memory + " != " + calculateMemory();
            return memory;
        }
        return 0;
    }

    /**
     * Amount of used disk space in persistent case including child pages.
     *
     * @return amount of used disk space in persistent case
     */
    public long getDiskSpaceUsed() {
        long r = 0;
        if (isPersistent()) {
            r += diskSpaceUsed;
        }
        if (!isLeaf()) {
            for (int i = 0; i < getRawChildPageCount(); i++) {
                long pos = getChildPagePos(i);
                if (pos != 0) {
                    r += getChildPage(i).getDiskSpaceUsed();
                }
            }
        }
        return r;
    }

    /**
     * Increase estimated memory used in persistent case.
     *
     * @param mem additional memory size.
     */
    final void addMemory(int mem) {
        memory += mem;
    }

    /**
     * Recalculate estimated memory used in persistent case.
     */
    final void recalculateMemory() {
        assert isPersistent();
        memory = calculateMemory();
    }

    /**
     * Calculate estimated memory used in persistent case.
     *
     * @return memory in bytes
     */
    protected int calculateMemory() {
        int keyCount = getKeyCount();
        int mem = keyCount * MEMORY_POINTER;
        DataType keyType = map.getKeyType();
        for (int i = 0; i < keyCount; i++) {
            mem += keyType.getMemory(keys[i]);
        }
        return mem;
    }

    public boolean isComplete() {
        return true;
    }

    /**
     * Called when done with copying page.
     */
    public void setComplete() {}

    /**
     * Remove the page.
     */
    public final void removePage() {
        if(isPersistent()) {
            long p = pos;
            if (p == 0) {
                removedInMemory = true;
            }
            map.removePage(p, memory);
        }
    }

    /**
     * Update given CursorPos chain to correspond to "append point" in a B-tree rooted at this Page.
     *
     * @param cursorPos to update, presumably pointing to this Page
     * @return new head of the CursorPos chain
     */
    public abstract CursorPos getAppendCursorPos(CursorPos cursorPos);

    /**
     * Remove all page data recursively.
     */
    public abstract void removeAllRecursive();

    /**
     * Create array for keys storage.
     *
     * @param size number of entries
     * @return values array
     */
    private Object[] createKeyStorage(int size)
    {
        return new Object[size];
    }

    /**
     * Create array for values storage.
     *
     * @param size number of entries
     * @return values array
     */
    final Object[] createValueStorage(int size)
    {
        return new Object[size];
    }

    /**
     * A pointer to a page, either in-memory or using a page position.
     */
    public static final class PageReference {

        /**
         * Singleton object used when arrays of PageReference have not yet been filled.
         */
        public static final PageReference EMPTY = new PageReference(null, 0, 0);

        /**
         * The position, if known, or 0.
         */
        private long pos;

        /**
         * The page, if in memory, or null.
         */
        private Page page;

        /**
         * The descendant count for this child page.
         */
        final long count;

        public PageReference(Page page) {
            this(page, page.getPos(), page.getTotalCount());
        }

        PageReference(long pos, long count) {
            this(null, pos, count);
            assert DataUtils.isPageSaved(pos);
        }

        private PageReference(Page page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

        public Page getPage() {
            return page;
        }

        /**
         * Clear if necessary, reference to the actual child Page object,
         * so it can be garbage collected if not actively used elsewhere.
         * Reference is cleared only if corresponding page was already saved on a disk.
         */
        void clearPageReference() {
            if (page != null) {
                page.writeEnd();
                assert page.isSaved() || !page.isComplete();
                if (page.isSaved()) {
                    assert pos == page.getPos();
                    assert count == page.getTotalCount() : count + " != " + page.getTotalCount();
                    page = null;
                }
            }
        }

        long getPos() {
            return pos;
        }

        /**
         * Re-acquire position from in-memory page.
         */
        void resetPos() {
            Page p = page;
            if (p != null && p.isSaved()) {
                pos = p.getPos();
                assert count == p.getTotalCount();
            }
        }

        @Override
        public String toString() {
            return "Cnt:" + count + ", pos:" + DataUtils.getPageChunkId(pos) +
                    "-" + DataUtils.getPageOffset(pos) + ":" + DataUtils.getPageMaxLength(pos) +
                    (page == null ? DataUtils.getPageType(pos) == 0 : page.isLeaf() ? " leaf" : " node") + ", " + page;
        }
    }


    private static class NonLeaf extends Page
    {
        /**
         * The child page references.
         */
        private PageReference[] children;

        /**
        * The total entry count of this page and all children.
        */
        private long totalCount;

        NonLeaf(MVMap<?, ?> map) {
            super(map);
        }

        NonLeaf(MVMap<?, ?> map, NonLeaf source, PageReference[] children, long totalCount) {
            super(map, source);
            this.children = children;
            this.totalCount = totalCount;
        }

        NonLeaf(MVMap<?, ?> map, Object[] keys, PageReference[] children, long totalCount) {
            super(map, keys);
            this.children = children;
            this.totalCount = totalCount;
        }

        @Override
        public int getNodeType() {
            return DataUtils.PAGE_TYPE_NODE;
        }

        @Override
        public Page copy(MVMap<?, ?> map) {
            return new IncompleteNonLeaf(map, this);
        }

        @Override
        public Page getChildPage(int index) {
            PageReference ref = children[index];
            Page page = ref.getPage();
            if(page == null) {
                page = map.readPage(ref.getPos());
                assert ref.getPos() == page.getPos();
                assert ref.count == page.getTotalCount();
            }
            return page;
        }

        @Override
        public Page getChildPageIfLoaded(int index) {
            return children[index].getPage();
        }

        @Override
        public long getChildPagePos(int index) {
            return children[index].getPos();
        }

        @Override
        public Object getValue(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            Object[] bKeys = splitKeys(at, b - 1);
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
            Page newPage = createNode(map, bKeys, bChildren, t, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public void expand(int keyCount, Object[] extraKeys, Object[] extraValues) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTotalCount() {
            assert !isComplete() || totalCount == calculateTotalCount() :
                        "Total count: " + totalCount + " != " + calculateTotalCount();
            return totalCount;
        }

        private long calculateTotalCount() {
            long check = 0;
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                check += children[i].count;
            }
            return check;
        }

        protected void recalculateTotalCount() {
            totalCount = calculateTotalCount();
        }

        @Override
        long getCounts(int index) {
            return children[index].count;
        }

        @Override
        public void setChild(int index, Page c) {
            assert c != null;
            PageReference child = children[index];
            if (c != child.getPage() || c.getPos() != child.getPos()) {
                totalCount += c.getTotalCount() - child.count;
                children = children.clone();
                children[index] = new PageReference(c);
            }
        }

        @Override
        public Object setValue(int index, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertLeaf(int index, Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertNode(int index, Object key, Page childPage) {
            int childCount = getRawChildPageCount();
            insertKey(index, key);

            PageReference[] newChildren = new PageReference[childCount + 1];
            DataUtils.copyWithGap(children, newChildren, childCount, index);
            children = newChildren;
            children[index] = new PageReference(childPage);

            totalCount += childPage.getTotalCount();
            if (isPersistent()) {
                addMemory(MEMORY_POINTER + PAGE_MEMORY_CHILD);
            }
        }

        @Override
        public void remove(int index) {
            int childCount = getRawChildPageCount();
            super.remove(index);
            if(isPersistent()) {
                addMemory(-MEMORY_POINTER - PAGE_MEMORY_CHILD);
            }
            totalCount -= children[index].count;
            PageReference[] newChildren = new PageReference[childCount - 1];
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;
        }

        @Override
        public void removeAllRecursive() {
            if (isPersistent()) {
                for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                    PageReference ref = children[i];
                    Page page = ref.getPage();
                    if (page != null) {
                        page.removeAllRecursive();
                    } else {
                        long c = ref.getPos();
                        int type = DataUtils.getPageType(c);
                        if (type == PAGE_TYPE_LEAF) {
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

        @Override
        public CursorPos getAppendCursorPos(CursorPos cursorPos) {
            int keyCount = getKeyCount();
            Page childPage = getChildPage(keyCount);
            return childPage.getAppendCursorPos(new CursorPos(this, keyCount, cursorPos));
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            children = new PageReference[keyCount + 1];
            long[] p = new long[keyCount + 1];
            for (int i = 0; i <= keyCount; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= keyCount; i++) {
                long s = DataUtils.readVarLong(buff);
                long position = p[i];
                assert position == 0 ? s == 0 : s >= 0;
                total += s;
                children[i] = position == 0 ? PageReference.EMPTY : new PageReference(position, s);
            }
            totalCount = total;
        }

        @Override
        protected void writeValues(WriteBuffer buff) {}

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                buff.putLong(children[i].getPos());
            }
            if(withCounts) {
                for (int i = 0; i <= keyCount; i++) {
                    buff.putVarLong(children[i].count);
                }
            }
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (!isSaved()) {
                int patch = write(chunk, buff);
                writeChildrenRecursive(chunk, buff);
                int old = buff.position();
                buff.position(patch);
                writeChildren(buff, false);
                buff.position(old);
            }
        }

        void writeChildrenRecursive(Chunk chunk, WriteBuffer buff) {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                PageReference ref = children[i];
                Page p = ref.getPage();
                if (p != null) {
                    p.writeUnsavedRecursive(chunk, buff);
                    ref.resetPos();
                }
            }
        }

        @Override
        void writeEnd() {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                children[i].clearPageReference();
            }
        }

        @Override
        public int getRawChildPageCount() {
            return getKeyCount() + 1;
        }

        @Override
        protected int calculateMemory() {
            return super.calculateMemory() + PAGE_NODE_MEMORY +
                        getRawChildPageCount() * (MEMORY_POINTER + PAGE_MEMORY_CHILD);
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append("[").append(Long.toHexString(children[i].getPos())).append("]");
                if(i < keyCount) {
                    buff.append(" ").append(getKey(i));
                }
            }
        }
    }


    private static class IncompleteNonLeaf extends NonLeaf {

        private boolean complete;

        IncompleteNonLeaf(MVMap<?, ?> map, NonLeaf source) {
            super(map, source, constructEmptyPageRefs(source.getRawChildPageCount()), source.getTotalCount());
        }

        private static PageReference[] constructEmptyPageRefs(int size) {
            // replace child pages with empty pages
            PageReference[] children = new PageReference[size];
            Arrays.fill(children, PageReference.EMPTY);
            return children;
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (complete) {
                super.writeUnsavedRecursive(chunk, buff);
            } else if (!isSaved()) {
                writeChildrenRecursive(chunk, buff);
            }
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public void setComplete() {
            recalculateTotalCount();
            complete = true;
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            buff.append(", complete:").append(complete);
        }

    }


    private static class Leaf extends Page
    {
        /**
         * The storage for values.
         */
        private Object[] values;

        Leaf(MVMap<?, ?> map) {
            super(map);
        }

        private Leaf(MVMap<?, ?> map, Leaf source) {
            super(map, source);
            this.values = source.values;
        }

        Leaf(MVMap<?, ?> map, Object[] keys, Object[] values) {
            super(map, keys);
            this.values = values;
        }

        @Override
        public int getNodeType() {
            return PAGE_TYPE_LEAF;
        }

        @Override
        public Page copy(MVMap<?, ?> map) {
            return new Leaf(map, this);
        }

        @Override
        public Page getChildPage(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page getChildPageIfLoaded(int index) { throw new UnsupportedOperationException(); }

        @Override
        public long getChildPagePos(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getValue(int index) {
            return values[index];
        }

        @Override
        public Page split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            Object[] bKeys = splitKeys(at, b);
            Object[] bValues = createValueStorage(b);
            if(values != null) {
                Object[] aValues = createValueStorage(at);
                System.arraycopy(values, 0, aValues, 0, at);
                System.arraycopy(values, at, bValues, 0, b);
                values = aValues;
            }
            Page newPage = createLeaf(map, bKeys, bValues, 0);
            if(isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public void expand(int extraKeyCount, Object[] extraKeys, Object[] extraValues) {
            int keyCount = getKeyCount();
            expandKeys(extraKeyCount, extraKeys);
            if(values != null) {
                Object[] newValues = createValueStorage(keyCount + extraKeyCount);
                System.arraycopy(values, 0, newValues, 0, keyCount);
                System.arraycopy(extraValues, 0, newValues, keyCount, extraKeyCount);
                values = newValues;
            }
            if(isPersistent()) {
                recalculateMemory();
            }
        }

        @Override
        public long getTotalCount() {
            return getKeyCount();
        }

        @Override
        long getCounts(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChild(int index, Page c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object setValue(int index, Object value) {
            DataType valueType = map.getValueType();
            values = values.clone();
            Object old = setValueInternal(index, value);
            if(isPersistent()) {
                addMemory(valueType.getMemory(value) -
                            valueType.getMemory(old));
            }
            return old;
        }

        private Object setValueInternal(int index, Object value) {
            Object old = values[index];
            values[index] = value;
            return old;
        }

        @Override
        public void insertLeaf(int index, Object key, Object value) {
            int keyCount = getKeyCount();
            insertKey(index, key);

            if(values != null) {
                Object[] newValues = createValueStorage(keyCount + 1);
                DataUtils.copyWithGap(values, newValues, keyCount, index);
                values = newValues;
                setValueInternal(index, value);
                if (isPersistent()) {
                    addMemory(MEMORY_POINTER + map.getValueType().getMemory(value));
                }
            }
        }

        @Override
        public void insertNode(int index, Object key, Page childPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(int index) {
            int keyCount = getKeyCount();
            super.remove(index);
            if (values != null) {
                if(isPersistent()) {
                    Object old = getValue(index);
                    addMemory(-MEMORY_POINTER - map.getValueType().getMemory(old));
                }
                Object[] newValues = createValueStorage(keyCount - 1);
                DataUtils.copyExcept(values, newValues, keyCount, index);
                values = newValues;
            }
        }

        @Override
        public void removeAllRecursive() {
            removePage();
        }

        @Override
        public CursorPos getAppendCursorPos(CursorPos cursorPos) {
            int keyCount = getKeyCount();
            return new CursorPos(this, -keyCount - 1, cursorPos);
        }

        @Override
        protected void readPayLoad(ByteBuffer buff) {
            int keyCount = getKeyCount();
            values = createValueStorage(keyCount);
            map.getValueType().read(buff, values, getKeyCount(), false);
        }

        @Override
        protected void writeValues(WriteBuffer buff) {
            map.getValueType().write(buff, values, getKeyCount(), false);
        }

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {}

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
            if (!isSaved()) {
                write(chunk, buff);
            }
        }

        @Override
        void writeEnd() {}

        @Override
        public int getRawChildPageCount() {
            return 0;
        }

        @Override
        protected int calculateMemory() {
            int keyCount = getKeyCount();
            int mem = super.calculateMemory() + PAGE_LEAF_MEMORY + keyCount * MEMORY_POINTER;
            DataType valueType = map.getValueType();
            for (int i = 0; i < keyCount; i++) {
                mem += valueType.getMemory(values[i]);
            }
            return mem;
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append(getKey(i));
                if (values != null) {
                    buff.append(':');
                    buff.append(getValue(i));
                }
            }
        }
    }
}
