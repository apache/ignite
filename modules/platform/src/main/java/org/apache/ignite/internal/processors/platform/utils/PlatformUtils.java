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

package org.apache.ignite.internal.processors.platform.utils;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.portable.*;
import org.apache.ignite.internal.processors.platform.*;
import org.apache.ignite.internal.processors.platform.memory.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.event.*;
import java.util.*;

/**
 * Platform utility methods.
 */
@SuppressWarnings({"UnusedDeclaration", "unchecked"})
public class PlatformUtils {
    /** Amount of peek modes available. */
    private static final int CACHE_PEEK_MODES_CNT = CachePeekMode.values().length;

    /** Cache peek modes. */
    private static volatile CachePeekMode[][] CACHE_PEEK_MODES;

    /**
     * Static initializer.
     */
    static {
        int len = 1 << CACHE_PEEK_MODES_CNT;

        synchronized (PlatformUtils.class) {
            CACHE_PEEK_MODES = new CachePeekMode[len][];

            CACHE_PEEK_MODES[0] = new CachePeekMode[0];
        }
    }

    /**
     * Write nullable collection to the writer.
     *
     * @param writer Writer.
     * @param col Collection to write.
     */
    public static <T> void writeNullableCollection(PortableRawWriterEx writer, @Nullable Collection<T> col) {
        writeNullableCollection(writer, col, null, null);
    }

    /**
     * Write nullable collection to the writer.
     *
     * @param writer Writer.
     * @param col Collection to write.
     * @param writeClo Writer closure.
     */
    public static <T> void writeNullableCollection(PortableRawWriterEx writer, @Nullable Collection<T> col,
        @Nullable PlatformWriterClosure<T> writeClo) {
        writeNullableCollection(writer, col, writeClo, null);
    }

    /**
     * Write collection to the writer.
     *
     * @param writer Writer.
     * @param col Collection to write.
     * @param writeClo Optional writer closure.
     * @param filter Optional filter.
     */
    public static <T> void writeNullableCollection(PortableRawWriterEx writer, @Nullable Collection<T> col,
        @Nullable PlatformWriterClosure<T> writeClo, @Nullable IgnitePredicate<T> filter) {
        if (col != null) {
            writer.writeBoolean(true);

            writeCollection(writer, col, writeClo, filter);
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Write collection to the writer.
     *
     * @param writer Writer.
     * @param col Collection to write.
     */
    public static <T> void writeCollection(PortableRawWriterEx writer, Collection<T> col) {
        writeCollection(writer, col, null, null);
    }

    /**
     * Write collection to the writer.
     *
     * @param writer Writer.
     * @param col Collection to write.
     * @param writeClo Writer closure.
     */
    public static <T> void writeCollection(PortableRawWriterEx writer, Collection<T> col,
        @Nullable PlatformWriterClosure<T> writeClo) {
        writeCollection(writer, col, writeClo, null);
    }

    /**
     * Write collection to the writer.
     *
     * @param writer Writer.
     * @param col Collection to write.
     * @param writeClo Optional writer closure.
     * @param filter Optional filter.
     */
    public static <T> void writeCollection(PortableRawWriterEx writer, Collection<T> col,
        @Nullable PlatformWriterClosure<T> writeClo, @Nullable IgnitePredicate<T> filter) {
        assert col != null;

        if (filter == null) {
            writer.writeInt(col.size());

            if (writeClo == null) {
                for (T entry : col)
                    writer.writeObject(entry);
            }
            else {
                for (T entry : col)
                    writeClo.write(writer, entry);
            }
        }
        else {
            int pos = writer.reserveInt();
            int cnt = 0;

            for (T entry : col) {
                if (filter.apply(entry)) {
                    cnt++;

                    if (writeClo == null)
                        writer.writeObject(entry);
                    else
                        writeClo.write(writer, entry);
                }
            }

            writer.writeInt(pos, cnt);
        }
    }

    /**
     * Write nullable map to the writer.
     *
     * @param writer Writer.
     * @param map Map to write.
     */
    public static <K, V> void writeNullableMap(PortableRawWriterEx writer, @Nullable Map<K, V> map) {
        if (map != null) {
            writer.writeBoolean(true);

            writeMap(writer, map);
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Write nullable map to the writer.
     *
     * @param writer Writer.
     * @param map Map to write.
     */
    public static <K, V> void writeMap(PortableRawWriterEx writer, Map<K, V> map) {
        assert map != null;

        writeMap(writer, map, null);
    }

    /**
     * Write nullable map to the writer.
     *
     * @param writer Writer.
     * @param map Map to write.
     * @param writeClo Writer closure.
     */
    public static <K, V> void writeMap(PortableRawWriterEx writer, Map<K, V> map,
        @Nullable PlatformWriterBiClosure<K, V> writeClo) {
        assert map != null;

        writer.writeInt(map.size());

        if (writeClo == null) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                writer.writeObject(entry.getKey());
                writer.writeObject(entry.getValue());
            }
        }
        else {
            for (Map.Entry<K, V> entry : map.entrySet())
                writeClo.write(writer, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Read collection.
     *
     * @param reader Reader.
     * @return List.
     */
    public static <T> List<T> readCollection(PortableRawReaderEx reader) {
        return readCollection(reader, null);
    }

    /**
     * Read collection.
     *
     * @param reader Reader.
     * @param readClo Optional reader closure.
     * @return List.
     */
    public static <T> List<T> readCollection(PortableRawReaderEx reader, @Nullable PlatformReaderClosure<T> readClo) {
        int cnt = reader.readInt();

        List<T> res = new ArrayList<>(cnt);

        if (readClo == null) {
            for (int i = 0; i < cnt; i++)
                res.add((T)reader.readObjectDetached());
        }
        else {
            for (int i = 0; i < cnt; i++)
                res.add(readClo.read(reader));
        }

        return res;
    }

    /**
     * Read nullable collection.
     *
     * @param reader Reader.
     * @return List.
     */
    public static <T> List<T> readNullableCollection(PortableRawReaderEx reader) {
        return readNullableCollection(reader, null);
    }

    /**
     * Read nullable collection.
     *
     * @param reader Reader.
     * @return List.
     */
    public static <T> List<T> readNullableCollection(PortableRawReaderEx reader,
        @Nullable PlatformReaderClosure<T> readClo) {
        if (!reader.readBoolean())
            return null;

        return readCollection(reader, readClo);
    }

    /**
     * @param reader Reader.
     * @return Set.
     */
    public static <T> Set<T> readSet(PortableRawReaderEx reader) {
        int cnt = reader.readInt();

        Set<T> res = U.newHashSet(cnt);

        for (int i = 0; i < cnt; i++)
            res.add((T)reader.readObjectDetached());

        return res;
    }

    /**
     * @param reader Reader.
     * @return Set.
     */
    public static <T> Set<T> readNullableSet(PortableRawReaderEx reader) {
        if (!reader.readBoolean())
            return null;

        return readSet(reader);
    }

    /**
     * Read map.
     *
     * @param reader Reader.
     * @return Map.
     */
    public static <K, V> Map<K, V> readMap(PortableRawReaderEx reader) {
        return readMap(reader, null);
    }

    /**
     * Read map.
     *
     * @param reader Reader.
     * @param readClo Reader closure.
     * @return Map.
     */
    public static <K, V> Map<K, V> readMap(PortableRawReaderEx reader,
        @Nullable PlatformReaderBiClosure<K, V> readClo) {
        int cnt = reader.readInt();

        Map<K, V> map = U.newHashMap(cnt);

        if (readClo == null) {
            for (int i = 0; i < cnt; i++)
                map.put((K)reader.readObjectDetached(), (V)reader.readObjectDetached());
        }
        else {
            for (int i = 0; i < cnt; i++) {
                IgniteBiTuple<K, V> entry = readClo.read(reader);

                map.put(entry.getKey(), entry.getValue());
            }
        }

        return map;
    }

    /**
     * Read nullable map.
     *
     * @param reader Reader.
     * @return Map.
     */
    public static <K, V> Map<K, V> readNullableMap(PortableRawReaderEx reader) {
        if (!reader.readBoolean())
            return null;

        return readMap(reader);
    }

    /**
     * Writes IgniteUuid to a writer.
     *
     * @param writer Writer.
     * @param val Values.
     */
    public static void writeIgniteUuid(PortableRawWriterEx writer, IgniteUuid val) {
        if (val == null)
            writer.writeUuid(null);
        else {
            writer.writeUuid(val.globalId());
            writer.writeLong(val.localId());
        }
    }

    /**
     * Convert native cache peek modes to Java cache peek modes.
     *
     * @param modes Encoded peek modes.
     * @return Cache peek modes.
     */
    public static CachePeekMode[] decodeCachePeekModes(int modes) {
        // 1. Try getting cache value.
        CachePeekMode[] res = CACHE_PEEK_MODES[modes];

        if (res == null) {
            // 2. Calculate modes from scratch.
            List<CachePeekMode> res0 = new ArrayList<>(CACHE_PEEK_MODES_CNT);

            for (int i = 0; i < CACHE_PEEK_MODES_CNT; i++) {
                int mask = 1 << i;

                if ((modes & mask) == mask)
                    res0.add(CachePeekMode.fromOrdinal((byte)i));
            }

            res = res0.toArray(new CachePeekMode[res0.size()]);

            synchronized (PlatformUtils.class) {
                CACHE_PEEK_MODES[modes] = res;
            }
        }

        return res;
    }

    /**
     * Unwrap query exception.
     *
     * @param err Initial error.
     * @return Unwrapped error.
     */
    public static IgniteCheckedException unwrapQueryException(Throwable err) {
        assert err != null;

        Throwable parent = err;
        Throwable child = parent.getCause();

        while (true) {
            if (child == null || child == parent)
                break;

            if (child instanceof IgniteException || child instanceof IgniteCheckedException
                || child instanceof CacheException) {
                // Continue unwrapping.
                parent = child;

                child = parent.getCause();

                continue;
            }

            break;
        }

        // Specific exception found, but detailed message doesn't exist. Just pass exception name then.
        if (parent.getMessage() == null)
            return new IgniteCheckedException("Query execution failed due to exception: " +
                parent.getClass().getName(), err);

        return new IgniteCheckedException(parent.getMessage(), err);
    }

    /**
     * Apply continuous query events to listener.
     *
     * @param ctx Interop context.
     * @param lsnrPtr Listener pointer.
     * @param evts Events.
     * @throws javax.cache.event.CacheEntryListenerException In case of failure.
     */
    public static void applyContinuousQueryEvents(PlatformContext ctx, long lsnrPtr, Iterable<CacheEntryEvent> evts)
        throws CacheEntryListenerException {
        assert lsnrPtr != 0;
        assert evts != null;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = ctx.writer(out);

            int cntPos = writer.reserveInt();

            int cnt = 0;

            for (CacheEntryEvent evt : evts) {
                writeCacheEntryEvent(writer, evt);

                cnt++;
            }

            writer.writeInt(cntPos, cnt);

            out.synchronize();

            ctx.gateway().continuousQueryListenerApply(lsnrPtr, mem.pointer());
        }
        catch (Exception e) {
            throw toCacheEntryListenerException(e);
        }
    }

    /**
     * Evaluate the filter.
     *
     * @param ctx Interop context.
     * @param filterPtr Native filter pointer.
     * @param evt Event.
     * @return Result.
     * @throws CacheEntryListenerException In case of failure.
     */
    public static boolean evaluateContinuousQueryEvent(PlatformContext ctx, long filterPtr, CacheEntryEvent evt)
        throws CacheEntryListenerException {
        assert filterPtr != 0;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            writeCacheEntryEvent(ctx.writer(out), evt);

            out.synchronize();

            return ctx.gateway().continuousQueryFilterApply(filterPtr, mem.pointer()) == 1;
        }
        catch (Exception e) {
            throw toCacheEntryListenerException(e);
        }
    }

    /**
     * Convert exception into listener exception.
     *
     * @param e Listener exception.
     * @return Exception.
     */
    private static CacheEntryListenerException toCacheEntryListenerException(Exception e) {
        assert e != null;

        return e instanceof CacheEntryListenerException ? (CacheEntryListenerException)e : e.getMessage() != null ?
            new CacheEntryListenerException(e.getMessage(), e) : new CacheEntryListenerException(e);
    }

    /**
     * Write event to the writer.
     *
     * @param writer Writer.
     * @param evt Event.
     */
    private static void writeCacheEntryEvent(PortableRawWriterEx writer, CacheEntryEvent evt) {
        writer.writeObjectDetached(evt.getKey());
        writer.writeObjectDetached(evt.getOldValue());
        writer.writeObjectDetached(evt.getValue());
    }

    /**
     * Writer error data.
     *
     * @param err Error.
     * @param writer Writer.
     */
    public static void writeErrorData(Throwable err, PortableRawWriterEx writer) {
        writeErrorData(err, writer, null);
    }

    /**
     * Write error data.
     * @param err Error.
     * @param writer Writer.
     * @param log Optional logger.
     */
    public static void writeErrorData(Throwable err, PortableRawWriterEx writer, @Nullable IgniteLogger log) {
        // Write additional data if needed.
        if (err instanceof PlatformExtendedException) {
            PlatformExtendedException err0 = (PlatformExtendedException)err;

            writer.writeBoolean(true); // Data exists.

            int pos = writer.out().position();

            try {
                writer.writeBoolean(true); // Optimistically assume that we will be able to write it.
                err0.writeData(writer);
            }
            catch (Exception e) {
                if (log != null)
                    U.warn(log, "Failed to write interop exception data: " + e.getMessage(), e);

                writer.out().position(pos);

                writer.writeBoolean(false); // Error occurred.
                writer.writeString(e.getClass().getName());

                String innerMsg;

                try {
                    innerMsg = e.getMessage();
                }
                catch (Exception innerErr) {
                    innerMsg = "Exception message is not available.";
                }

                writer.writeString(innerMsg);
            }
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Private constructor.
     */
    private PlatformUtils() {
        // No-op.
    }
}
