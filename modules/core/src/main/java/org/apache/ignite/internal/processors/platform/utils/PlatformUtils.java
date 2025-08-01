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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cacheobject.PlatformCacheObjectImpl;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformExtendedException;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetServiceImpl;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.service.LazyServiceConfiguration;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PREFIX;

/**
 * Platform utility methods.
 */
public class PlatformUtils {
    /** Node attribute: platform. */
    public static final String ATTR_PLATFORM = ATTR_PREFIX + ".platform";

    /** Platform: CPP. */
    public static final String PLATFORM_CPP = "cpp";

    /** Platform: .Net. */
    public static final String PLATFORM_DOTNET = "dotnet";

    /** Operation: prepare .Net platform. */
    public static final int OP_PREPARE_DOT_NET = 1;

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
    public static <T> void writeNullableCollection(BinaryWriterEx writer, @Nullable Collection<T> col) {
        writeNullableCollection(writer, col, null, null);
    }

    /**
     * Write nullable collection to the writer.
     *
     * @param writer Writer.
     * @param col Collection to write.
     * @param writeClo Writer closure.
     */
    public static <T> void writeNullableCollection(BinaryWriterEx writer, @Nullable Collection<T> col,
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
    public static <T> void writeNullableCollection(BinaryWriterEx writer, @Nullable Collection<T> col,
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
    public static <T> void writeCollection(BinaryWriterEx writer, Collection<T> col) {
        writeCollection(writer, col, null, null);
    }

    /**
     * Write collection to the writer.
     *
     * @param writer Writer.
     * @param col Collection to write.
     * @param writeClo Writer closure.
     */
    public static <T> void writeCollection(BinaryWriterEx writer, Collection<T> col,
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
    public static <T> void writeCollection(BinaryWriterEx writer, Collection<T> col,
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
    public static <K, V> void writeNullableMap(BinaryWriterEx writer, @Nullable Map<K, V> map) {
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
    public static <K, V> void writeMap(BinaryWriterEx writer, Map<K, V> map) {
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
    public static <K, V> void writeMap(BinaryWriterEx writer, Map<K, V> map,
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
    public static <T> List<T> readCollection(BinaryReaderEx reader) {
        return readCollection(reader, null);
    }

    /**
     * Read collection.
     *
     * @param reader Reader.
     * @param readClo Optional reader closure.
     * @return List.
     */
    public static <T> List<T> readCollection(BinaryReaderEx reader, @Nullable PlatformReaderClosure<T> readClo) {
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
    public static <T> List<T> readNullableCollection(BinaryReaderEx reader) {
        return readNullableCollection(reader, null);
    }

    /**
     * Read nullable collection.
     *
     * @param reader Reader.
     * @return List.
     */
    public static <T> List<T> readNullableCollection(BinaryReaderEx reader,
        @Nullable PlatformReaderClosure<T> readClo) {
        if (!reader.readBoolean())
            return null;

        return readCollection(reader, readClo);
    }

    /**
     * @param reader Reader.
     * @return Set.
     */
    public static <T> Set<T> readSet(BinaryReaderEx reader) {
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
    public static <T> Set<T> readNullableSet(BinaryReaderEx reader) {
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
    public static <K, V> Map<K, V> readMap(BinaryReaderEx reader) {
        return readMap(reader, null);
    }

    /**
     * Read map.
     *
     * @param reader Reader.
     * @param readClo Reader closure.
     * @return Map.
     */
    public static <K, V> Map<K, V> readMap(BinaryReaderEx reader,
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
     * Read linked map.
     *
     * @param reader Reader.
     * @param readClo Reader closure.
     * @return Map.
     */
    public static <K, V> Map<K, V> readLinkedMap(BinaryReaderEx reader,
        @Nullable PlatformReaderBiClosure<K, V> readClo) {
        int cnt = reader.readInt();

        Map<K, V> map = U.newLinkedHashMap(cnt);

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
    public static <K, V> Map<K, V> readNullableMap(BinaryReaderEx reader) {
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
    public static void writeIgniteUuid(BinaryWriterEx writer, IgniteUuid val) {
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
     * @param ctx Context.
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

            BinaryWriterEx writer = ctx.writer(out);

            writer.writeLong(lsnrPtr);

            int cntPos = writer.reserveInt();

            int cnt = 0;

            for (CacheEntryEvent evt : evts) {
                writeCacheEntryEvent(writer, evt);

                cnt++;
            }

            writer.writeInt(cntPos, cnt);

            out.synchronize();

            ctx.gateway().continuousQueryListenerApply(mem.pointer());
        }
        catch (Exception e) {
            throw toCacheEntryListenerException(e);
        }
    }

    /**
     * Evaluate the filter.
     *
     * @param ctx Context.
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

            out.writeLong(filterPtr);

            writeCacheEntryEvent(ctx.writer(out), evt);

            out.synchronize();

            return ctx.gateway().continuousQueryFilterApply(mem.pointer()) == 1;
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
    private static void writeCacheEntryEvent(BinaryWriterEx writer, CacheEntryEvent evt) {
        writer.writeObjectDetached(evt.getKey());
        writer.writeObjectDetached(evt.getOldValue());
        writer.writeObjectDetached(evt.getValue());
        writeEventType(writer, evt.getEventType());
    }

    /**
     * Write event type to the writer.
     * @param writer Writer.
     * @param evtType Type of event.
     */
    private static void writeEventType(BinaryWriterEx writer, EventType evtType) {
        switch (evtType) {
            case CREATED: writer.writeByte((byte)0); break;
            case UPDATED: writer.writeByte((byte)1); break;
            case REMOVED: writer.writeByte((byte)2); break;
            case EXPIRED: writer.writeByte((byte)3); break;
            default:
                throw new IllegalArgumentException("Unknown event type: " + evtType);
        }
    }

    /**
     * Writes error.
     *
     * @param ex Error.
     * @param writer Writer.
     */
    public static void writeError(Throwable ex, BinaryWriterEx writer) {
        writer.writeObjectDetached(ex.getClass().getName());

        writer.writeObjectDetached(ex.getMessage());

        writer.writeObjectDetached(X.getFullStackTrace(ex));

        PlatformNativeException nativeCause = X.cause(ex, PlatformNativeException.class);

        if (nativeCause != null) {
            writer.writeBoolean(true);

            writer.writeObjectDetached(nativeCause.cause());
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Writer error data.
     *
     * @param err Error.
     * @param writer Writer.
     */
    public static void writeErrorData(Throwable err, BinaryWriterEx writer) {
        writeErrorData(err, writer, null);
    }

    /**
     * Write error data.
     * @param err Error.
     * @param writer Writer.
     * @param log Optional logger.
     */
    public static void writeErrorData(Throwable err, BinaryWriterEx writer, @Nullable IgniteLogger log) {
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
                catch (Exception ignored) {
                    innerMsg = "Exception message is not available.";
                }

                writer.writeString(innerMsg);
            }
        }
        else
            writer.writeBoolean(false);
    }

    /**
     * Get platform processor.
     *
     * @param grid Ignite instance.
     * @return Platform processor.
     */
    public static PlatformProcessor platformProcessor(Ignite grid) {
        GridKernalContext ctx = ((IgniteKernal)grid).context();

        return ctx.platform();
    }

    /**
     * Gets interop context for the grid.
     *
     * @param grid Grid
     * @return Context.
     */
    public static PlatformContext platformContext(Ignite grid) {
        return platformProcessor(grid).context();
    }

    /**
     * Reallocate arbitrary memory chunk.
     *
     * @param memPtr Memory pointer.
     * @param cap Capacity.
     */
    public static void reallocate(long memPtr, int cap) {
        PlatformMemoryUtils.reallocate(memPtr, cap);
    }

    /**
     * Get error data.
     *
     * @param err Error.
     * @return Error data.
     */
    public static byte[] errorData(Throwable err) {
        if (err instanceof PlatformExtendedException) {
            PlatformContext ctx = ((PlatformExtendedException)err).context();

            try (PlatformMemory mem = ctx.memory().allocate()) {
                // Write error data.
                PlatformOutputStream out = mem.output();

                BinaryWriterEx writer = ctx.writer(out);

                try {
                    PlatformUtils.writeErrorData(err, writer, ctx.kernalContext().log(PlatformContext.class));
                }
                finally {
                    out.synchronize();
                }

                // Read error data into separate array.
                PlatformInputStream in = mem.input();

                in.synchronize();

                int len = in.remaining();

                assert len > 0;

                byte[] arr = in.array();
                byte[] res = new byte[len];

                System.arraycopy(arr, 0, res, 0, len);

                return res;
            }
        }
        else
            return null;
    }

    /**
     * Writes invocation result (of a job/service/etc) using a common protocol.
     *
     * @param writer Writer.
     * @param resObj Result.
     * @param err Error.
     */
    public static void writeInvocationResult(BinaryWriterEx writer, Object resObj, Throwable err) {
        if (err == null) {
            writer.writeBoolean(true);
            writer.writeObject(resObj);
        }
        else {
            writer.writeBoolean(false);

            PlatformNativeException nativeErr = null;

            if (err instanceof IgniteCheckedException)
                nativeErr = ((IgniteCheckedException)err).getCause(PlatformNativeException.class);
            else if (err instanceof IgniteException)
                nativeErr = ((IgniteException)err).getCause(PlatformNativeException.class);

            if (nativeErr == null) {
                writer.writeBoolean(false);
                writer.writeString(err.getClass().getName());
                writer.writeString(err.getMessage());
                writer.writeString(X.getFullStackTrace(err));
            }
            else {
                writer.writeBoolean(true);
                writer.writeObject(nativeErr.cause());
            }
        }
    }

    /**
     * Reads invocation result (of a job/service/etc) using a common protocol.
     *
     * @param ctx Platform context.
     * @param reader Reader.
     * @return Result.
     * @throws IgniteCheckedException When invocation result is an error.
     */
    public static Object readInvocationResult(PlatformContext ctx, BinaryReaderEx reader)
        throws IgniteCheckedException {
        return readInvocationResult(ctx, reader, false);
    }

    /**
     * Reads invocation result (of a job/service/etc) using a common protocol.
     *
     * @param ctx Platform context.
     * @param reader Reader.
     * @param deserialize If {@code true} deserialize invocation result.
     * @return Result.
     * @throws IgniteCheckedException When invocation result is an error.
     */
    public static Object readInvocationResult(PlatformContext ctx, BinaryReaderEx reader, boolean deserialize)
            throws IgniteCheckedException {
        // 1. Read success flag.
        boolean success = reader.readBoolean();

        if (success)
            // 2. Return result as is.
            return deserialize ? reader.readObject() : reader.readObjectDetached();
        else {
            // 3. Read whether exception is in form of object or string.
            boolean hasEx = reader.readBoolean();

            if (hasEx) {
                // 4. Full exception.
                Object nativeErr = reader.readObjectDetached();

                assert nativeErr != null;

                throw ctx.createNativeException(nativeErr);
            }
            else {
                // 5. Native exception was not serializable, we have only message.
                String errMsg = reader.readString();

                assert errMsg != null;

                throw new IgniteCheckedException(errMsg);
            }
        }
    }

    /**
     * Create binary marshaller.
     *
     * @return Marshaller.
     */
    public static GridBinaryMarshaller marshaller() {
        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextImpl(null, null));

        return new GridBinaryMarshaller(U.binaryContext(BinaryUtils.noopMetadataHandler(), marsh));
    }

    /**
     * @param o Object to unwrap.
     * @return Unwrapped object.
     */
    private static Object unwrapBinary(Object o) {
        if (o == null)
            return null;

        if (knownArray(o))
            return o;

        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            Object uKey = unwrapBinary(key);

            Object val = entry.getValue();

            Object uVal = unwrapBinary(val);

            return (key != uKey || val != uVal) ? F.t(uKey, uVal) : o;
        }
        else if (BinaryUtils.knownCollection(o))
            return unwrapKnownCollection((Collection<Object>)o);
        else if (BinaryUtils.knownMap(o))
            return unwrapBinariesIfNeeded((Map<Object, Object>)o);
        else if (o instanceof Object[])
            return unwrapBinariesInArray((Object[])o);
        else if (o instanceof BinaryObject)
            return ((BinaryObject)o).deserialize();

        return o;
    }

    /**
     * @param obj Obj.
     * @return True is obj is a known simple type array.
     */
    private static boolean knownArray(Object obj) {
        return obj instanceof String[] ||
            obj instanceof boolean[] ||
            obj instanceof byte[] ||
            obj instanceof char[] ||
            obj instanceof int[] ||
            obj instanceof long[] ||
            obj instanceof short[] ||
            obj instanceof Timestamp[] ||
            obj instanceof double[] ||
            obj instanceof float[] ||
            obj instanceof UUID[] ||
            obj instanceof BigDecimal[];
    }

    /**
     * @param o Object to test.
     * @return True if collection should be recursively unwrapped.
     */
    private static boolean knownCollection(Object o) {
        Class<?> cls = o == null ? null : o.getClass();

        return cls == ArrayList.class || cls == LinkedList.class || cls == HashSet.class;
    }

    /**
     * @param o Object to test.
     * @return True if map should be recursively unwrapped.
     */
    private static boolean knownMap(Object o) {
        Class<?> cls = o == null ? null : o.getClass();

        return cls == HashMap.class || cls == LinkedHashMap.class;
    }

    /**
     * @param col Collection to unwrap.
     * @return Unwrapped collection.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private static Collection<Object> unwrapKnownCollection(Collection<Object> col) {
        Collection<Object> col0 = BinaryUtils.newKnownCollection(col);

        for (Object obj : col)
            col0.add(unwrapBinary(obj));

        return (col0 instanceof MutableSingletonList) ? U.convertToSingletonList(col0) : col0;
    }

    /**
     * Unwrap array of binaries if needed.
     *
     * @param arr Array.
     * @return Result.
     */
    public static Object[] unwrapBinariesInArray(Object[] arr) {
        if (arr.getClass().getComponentType() != Object.class)
            return arr;

        Object[] res = new Object[arr.length];

        for (int i = 0; i < arr.length; i++)
            res[i] = unwrapBinary(arr[i]);

        return res;
    }

    /**
     * Unwraps map.
     *
     * @param map Map to unwrap.
     * @return Unwrapped collection.
     */
    private static Map<Object, Object> unwrapBinariesIfNeeded(Map<Object, Object> map) {
        Map<Object, Object> map0 = BinaryUtils.newMap(map);

        for (Map.Entry<Object, Object> e : map.entrySet())
            map0.put(unwrapBinary(e.getKey()), unwrapBinary(e.getValue()));

        return map0;
    }

    /**
     * Create Java object.
     *
     * @param clsName Class name.
     * @return Instance.
     */
    public static <T> T createJavaObject(String clsName) {
        if (clsName == null)
            throw new IgniteException("Java object/factory class name is not set.");

        Class cls = U.classForName(clsName, null);

        if (cls == null)
            throw new IgniteException("Java object/factory class is not found (is it in the classpath?): " +
                clsName);

        try {
            return (T)cls.newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to instantiate Java object/factory class (does it have public " +
                "default constructor?): " + clsName, e);
        }
    }

    /**
     * Initialize Java object or object factory.
     *
     * @param obj Object.
     * @param clsName Class name.
     * @param props Properties (optional).
     * @param ctx Kernal context (optional).
     */
    public static void initializeJavaObject(Object obj, String clsName, @Nullable Map<String, Object> props,
        @Nullable GridKernalContext ctx) {
        if (props != null) {
            for (Map.Entry<String, Object> prop : props.entrySet()) {
                String fieldName = prop.getKey();

                if (fieldName == null)
                    throw new IgniteException("Java object/factory field name cannot be null: " + clsName);

                Field field = U.findField(obj.getClass(), fieldName);

                if (field == null)
                    throw new IgniteException("Java object/factory class field is not found [" +
                        "className=" + clsName + ", fieldName=" + fieldName + ']');

                Object val = prop.getValue();

                try {
                    field.set(
                        obj,
                        BinaryUtils.isObjectArray(field.getType()) ? BinaryUtils.rawArrayFromBinary(val) : val
                    );
                }
                catch (Exception e) {
                    throw new IgniteException("Failed to set Java object/factory field [className=" + clsName +
                        ", fieldName=" + fieldName + ", fieldValue=" + val + ']', e);
                }
            }
        }

        if (ctx != null) {
            try {
                ctx.resource().injectGeneric(obj);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to inject resources to Java factory: " + clsName, e);
            }
        }
    }

    /**
     * Gets the entire nested stack-trace of an throwable.
     *
     * @param throwable The {@code Throwable} to be examined.
     * @return The nested stack trace, with the root cause first.
     */
    public static String getFullStackTrace(Throwable throwable) {
        return X.getFullStackTrace(throwable);
    }

    /**
     * Writes the binary metadata to a writer.
     *
     * @param writer Writer.
     * @param meta Meta.
     */
    public static void writeBinaryMetadata(BinaryRawWriter writer, BinaryMetadata meta, boolean includeSchemas) {
        assert meta != null;

        Map<String, BinaryFieldMetadata> fields = meta.fieldsMap();

        writer.writeInt(meta.typeId());
        writer.writeString(meta.typeName());
        writer.writeString(meta.affinityKeyFieldName());

        writer.writeInt(fields.size());

        for (Map.Entry<String, BinaryFieldMetadata> e : fields.entrySet()) {
            writer.writeString(e.getKey());

            writer.writeInt(e.getValue().typeId());
            writer.writeInt(e.getValue().fieldId());
        }

        if (meta.isEnum()) {
            writer.writeBoolean(true);

            Map<String, Integer> enumMap = meta.enumMap();

            writer.writeInt(enumMap.size());

            for (Map.Entry<String, Integer> e: enumMap.entrySet()) {
                writer.writeString(e.getKey());
                writer.writeInt(e.getValue());
            }
        }
        else {
            writer.writeBoolean(false);
        }

        if (!includeSchemas) {
            return;
        }

        // Schemas.
        Collection<T2<Integer, int[]>> schemas = BinaryUtils.schemasAndFieldsIds(meta);

        writer.writeInt(schemas.size());

        for (T2<Integer, int[]> schema : schemas) {
            writer.writeInt(schema.get1());

            int[] ids = schema.get2();
            writer.writeInt(ids.length);

            for (int id : ids) {
                writer.writeInt(id);
            }
        }
    }

    /**
     * Reads the binary metadata.
     *
     * @param reader Reader.
     * @return Collection of metas.
     */
    public static Collection<BinaryMetadata> readBinaryMetadataCollection(BinaryReaderEx reader) {
        return readCollection(reader,
                new PlatformReaderClosure<BinaryMetadata>() {
                    @Override public BinaryMetadata read(BinaryReaderEx reader) {
                        return readBinaryMetadata(reader);
                    }
                }
        );
    }

    /**
     * Reads the binary metadata.
     *
     * @param reader Reader.
     * @return Binary type metadata.
     */
    public static BinaryMetadata readBinaryMetadata(BinaryReaderEx reader) {
        int typeId = reader.readInt();
        String typeName = reader.readString();
        String affKey = reader.readString();

        Map<String, BinaryFieldMetadata> fields = readLinkedMap(reader,
                new PlatformReaderBiClosure<String, BinaryFieldMetadata>() {
                    @Override public IgniteBiTuple<String, BinaryFieldMetadata> read(BinaryReaderEx reader) {
                        String name = reader.readString();
                        int typeId = reader.readInt();
                        int fieldId = reader.readInt();

                        return new IgniteBiTuple<String, BinaryFieldMetadata>(name,
                                new BinaryFieldMetadata(typeId, fieldId));
                    }
                });

        Map<String, Integer> enumMap = null;

        boolean isEnum = reader.readBoolean();

        if (isEnum) {
            int size = reader.readInt();

            enumMap = new LinkedHashMap<>(size);

            for (int idx = 0; idx < size; idx++)
                enumMap.put(reader.readString(), reader.readInt());
        }

        // Read schemas
        int schemaCnt = reader.readInt();

        List<T2<Integer, List<Integer>>> schemas = null;

        if (schemaCnt > 0) {
            schemas = new ArrayList<>(schemaCnt);

            for (int i = 0; i < schemaCnt; i++) {
                int id = reader.readInt();
                int fieldCnt = reader.readInt();
                List<Integer> fieldIds = new ArrayList<>(fieldCnt);

                for (int j = 0; j < fieldCnt; j++)
                    fieldIds.add(reader.readInt());

                schemas.add(new T2<>(id, fieldIds));
            }
        }

        return BinaryUtils.binaryMetadata(typeId, typeName, fields, affKey, schemas, isEnum, enumMap);
    }

    /**
     * Writes node attributes.
     *
     * @param writer Writer.
     * @param attrs Attributes.
     */
    public static void writeNodeAttributes(BinaryWriterEx writer, Map<String, Object> attrs) {
        assert writer != null;
        assert attrs != null;

        if (attrs != null) {
            writer.writeInt(attrs.size());

            for (Map.Entry<String, Object> e : attrs.entrySet()) {
                writer.writeString(e.getKey());
                writer.writeObjectDetached(e.getValue());
            }
        }
        else {
            writer.writeInt(0);
        }
    }

    /**
     * Reads node attributes.
     *
     * @param reader Reader.
     * @return Attributes.
     */
    public static Map<String, Object> readNodeAttributes(BinaryReaderEx reader) {
        assert reader != null;

        int attrCnt = reader.readInt();
        Map<String, Object> attrs = new HashMap<>(attrCnt);

        for (int j = 0; j < attrCnt; j++) {
            attrs.put(reader.readString(), reader.readObjectDetached());
        }

        return attrs;
    }

    /**
     * Write binary productVersion.
     *
     * @param out Writer.
     * @param productVersion IgniteProductVersion.
     */
    public static void writeNodeVersion(BinaryWriterEx out, IgniteProductVersion productVersion) {
        out.writeByte(productVersion.major());
        out.writeByte(productVersion.minor());
        out.writeByte(productVersion.maintenance());
        out.writeString(productVersion.stage());
        out.writeLong(productVersion.revisionTimestamp());
        out.writeByteArray(productVersion.revisionHash());
    }

    /**
     * Reads collection of strings.
     *
     * @param reader Reader.
     */
    public static Collection<String> readStrings(BinaryRawReader reader) {
        assert reader != null;

        int cnt = reader.readInt();

        Collection<String> strings = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            strings.add(reader.readString());

        return strings;
    }

    /**
     * Get service platform.
     *
     * @param svcCfg Service configuration.
     * @return Service platform or empty string if this is a java service.
     */
    public static String servicePlatform(ServiceConfiguration svcCfg) {
        if (svcCfg instanceof LazyServiceConfiguration) {
            String svcClsName = ((LazyServiceConfiguration)svcCfg).serviceClassName();

            if (PlatformDotNetServiceImpl.class.getName().equals(svcClsName))
                return PLATFORM_DOTNET;
        }
        else if (svcCfg instanceof ServiceConfiguration && svcCfg.getService() instanceof PlatformDotNetServiceImpl)
            return PLATFORM_DOTNET;

        return "";
    }

    /**
     * Read cache object from the stream as raw bytes to avoid marshalling.
     *
     * @param reader Reader.
     * @param isKey {@code True} if object is a key.
     */
    public static <T extends CacheObject> T readCacheObject(BinaryReaderEx reader, boolean isKey) {
        BinaryInputStream in = reader.in();

        int pos0 = in.position();

        Object obj = reader.readObjectDetached();

        if (obj == null)
            return null;

        if (obj instanceof CacheObject)
            return (T)obj;

        int pos1 = in.position();

        in.position(pos0);

        byte[] objBytes = in.readByteArray(pos1 - pos0);

        return isKey ?
            (T)new KeyCacheObjectImpl(obj, objBytes, -1) :
            (T)new PlatformCacheObjectImpl(obj, objBytes);
    }

    /**
     * Private constructor.
     */
    private PlatformUtils() {
        // No-op.
    }
}
