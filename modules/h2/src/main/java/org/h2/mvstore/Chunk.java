/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * A chunk of data, containing one or multiple pages.
 * <p>
 * Chunks are page aligned (each page is usually 4096 bytes).
 * There are at most 67 million (2^26) chunks,
 * each chunk is at most 2 GB large.
 */
public class Chunk {

    /**
     * The maximum chunk id.
     */
    public static final int MAX_ID = (1 << 26) - 1;

    /**
     * The maximum length of a chunk header, in bytes.
     */
    static final int MAX_HEADER_LENGTH = 1024;

    /**
     * The length of the chunk footer. The longest footer is:
     * chunk:ffffffff,block:ffffffffffffffff,
     * version:ffffffffffffffff,fletcher:ffffffff
     */
    static final int FOOTER_LENGTH = 128;

    /**
     * The chunk id.
     */
    public final int id;

    /**
     * The start block number within the file.
     */
    public long block;

    /**
     * The length in number of blocks.
     */
    public int len;

    /**
     * The total number of pages in this chunk.
     */
    public int pageCount;

    /**
     * The number of pages still alive.
     */
    public int pageCountLive;

    /**
     * The sum of the max length of all pages.
     */
    public long maxLen;

    /**
     * The sum of the max length of all pages that are in use.
     */
    public long maxLenLive;

    /**
     * The garbage collection priority. Priority 0 means it needs to be
     * collected, a high value means low priority.
     */
    public int collectPriority;

    /**
     * The position of the meta root.
     */
    public long metaRootPos;

    /**
     * The version stored in this chunk.
     */
    public long version;

    /**
     * When this chunk was created, in milliseconds after the store was created.
     */
    public long time;

    /**
     * When this chunk was no longer needed, in milliseconds after the store was
     * created. After this, the chunk is kept alive a bit longer (in case it is
     * referenced in older versions).
     */
    public long unused;

    /**
     * The last used map id.
     */
    public int mapId;

    /**
     * The predicted position of the next chunk.
     */
    public long next;

    Chunk(int id) {
        this.id = id;
    }

    /**
     * Read the header from the byte buffer.
     *
     * @param buff the source buffer
     * @param start the start of the chunk in the file
     * @return the chunk
     */
    static Chunk readChunkHeader(ByteBuffer buff, long start) {
        int pos = buff.position();
        byte[] data = new byte[Math.min(buff.remaining(), MAX_HEADER_LENGTH)];
        buff.get(data);
        try {
            for (int i = 0; i < data.length; i++) {
                if (data[i] == '\n') {
                    // set the position to the start of the first page
                    buff.position(pos + i + 1);
                    String s = new String(data, 0, i, StandardCharsets.ISO_8859_1).trim();
                    return fromString(s);
                }
            }
        } catch (Exception e) {
            // there could be various reasons
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupt reading chunk at position {0}", start, e);
        }
        throw DataUtils.newIllegalStateException(
                DataUtils.ERROR_FILE_CORRUPT,
                "File corrupt reading chunk at position {0}", start);
    }

    /**
     * Write the chunk header.
     *
     * @param buff the target buffer
     * @param minLength the minimum length
     */
    void writeChunkHeader(WriteBuffer buff, int minLength) {
        long pos = buff.position();
        buff.put(asString().getBytes(StandardCharsets.ISO_8859_1));
        while (buff.position() - pos < minLength - 1) {
            buff.put((byte) ' ');
        }
        if (minLength != 0 && buff.position() > minLength) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL,
                    "Chunk metadata too long");
        }
        buff.put((byte) '\n');
    }

    /**
     * Get the metadata key for the given chunk id.
     *
     * @param chunkId the chunk id
     * @return the metadata key
     */
    static String getMetaKey(int chunkId) {
        return "chunk." + Integer.toHexString(chunkId);
    }

    /**
     * Build a block from the given string.
     *
     * @param s the string
     * @return the block
     */
    public static Chunk fromString(String s) {
        HashMap<String, String> map = DataUtils.parseMap(s);
        int id = DataUtils.readHexInt(map, "chunk", 0);
        Chunk c = new Chunk(id);
        c.block = DataUtils.readHexLong(map, "block", 0);
        c.len = DataUtils.readHexInt(map, "len", 0);
        c.pageCount = DataUtils.readHexInt(map, "pages", 0);
        c.pageCountLive = DataUtils.readHexInt(map, "livePages", c.pageCount);
        c.mapId = DataUtils.readHexInt(map, "map", 0);
        c.maxLen = DataUtils.readHexLong(map, "max", 0);
        c.maxLenLive = DataUtils.readHexLong(map, "liveMax", c.maxLen);
        c.metaRootPos = DataUtils.readHexLong(map, "root", 0);
        c.time = DataUtils.readHexLong(map, "time", 0);
        c.unused = DataUtils.readHexLong(map, "unused", 0);
        c.version = DataUtils.readHexLong(map, "version", id);
        c.next = DataUtils.readHexLong(map, "next", 0);
        return c;
    }

    /**
     * Calculate the fill rate in %. 0 means empty, 100 means full.
     *
     * @return the fill rate
     */
    public int getFillRate() {
        if (maxLenLive <= 0) {
            return 0;
        } else if (maxLenLive == maxLen) {
            return 100;
        }
        return 1 + (int) (98 * maxLenLive / maxLen);
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Chunk && ((Chunk) o).id == id;
    }

    /**
     * Get the chunk data as a string.
     *
     * @return the string
     */
    public String asString() {
        StringBuilder buff = new StringBuilder(240);
        DataUtils.appendMap(buff, "chunk", id);
        DataUtils.appendMap(buff, "block", block);
        DataUtils.appendMap(buff, "len", len);
        if (maxLen != maxLenLive) {
            DataUtils.appendMap(buff, "liveMax", maxLenLive);
        }
        if (pageCount != pageCountLive) {
            DataUtils.appendMap(buff, "livePages", pageCountLive);
        }
        DataUtils.appendMap(buff, "map", mapId);
        DataUtils.appendMap(buff, "max", maxLen);
        if (next != 0) {
            DataUtils.appendMap(buff, "next", next);
        }
        DataUtils.appendMap(buff, "pages", pageCount);
        DataUtils.appendMap(buff, "root", metaRootPos);
        DataUtils.appendMap(buff, "time", time);
        if (unused != 0) {
            DataUtils.appendMap(buff, "unused", unused);
        }
        DataUtils.appendMap(buff, "version", version);
        return buff.toString();
    }

    byte[] getFooterBytes() {
        StringBuilder buff = new StringBuilder(FOOTER_LENGTH);
        DataUtils.appendMap(buff, "chunk", id);
        DataUtils.appendMap(buff, "block", block);
        DataUtils.appendMap(buff, "version", version);
        byte[] bytes = buff.toString().getBytes(StandardCharsets.ISO_8859_1);
        int checksum = DataUtils.getFletcher32(bytes, 0, bytes.length);
        DataUtils.appendMap(buff, "fletcher", checksum);
        while (buff.length() < FOOTER_LENGTH - 1) {
            buff.append(' ');
        }
        buff.append('\n');
        return buff.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    @Override
    public String toString() {
        return asString();
    }

}

