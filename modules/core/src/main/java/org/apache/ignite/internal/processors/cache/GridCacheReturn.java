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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.UnregisteredBinaryTypeException;
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Return value for cases where both, value and success flag need to be returned.
 */
public class GridCacheReturn implements Externalizable, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value. */
    @GridToStringInclude(sensitive = true)
    @GridDirectTransient
    private volatile Object v;

    /** */
    private CacheObject cacheObj;

    /** */
    @GridDirectCollection(CacheInvokeDirectResult.class)
    private Collection<CacheInvokeDirectResult> invokeResCol;

    /** Success flag. */
    private volatile boolean success;

    /** */
    private volatile boolean invokeRes;

    /** Local result flag, if non local then do not need unwrap cache objects. */
    @GridDirectTransient
    private transient boolean loc;

    /** */
    private int cacheId;

    /**
     * Empty constructor.
     */
    public GridCacheReturn() {
        loc = true;
    }

    /**
     * @param loc {@code True} if created on the node initiated cache operation.
     */
    public GridCacheReturn(boolean loc) {
        this.loc = loc;
    }

    /**
     * @param loc {@code True} if created on the node initiated cache operation.
     * @param success Success flag.
     */
    public GridCacheReturn(boolean loc, boolean success) {
        this.loc = loc;
        this.success = success;
    }

    /**
     * @param cctx Cache context.
     * @param loc {@code True} if created on the node initiated cache operation.
     * @param keepBinary True is deserialize value from a binary representation, false otherwise.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @param v Value.
     * @param success Success flag.
     */
    public GridCacheReturn(
        GridCacheContext cctx,
        boolean loc,
        boolean keepBinary,
        @Nullable ClassLoader ldr,
        Object v,
        boolean success
    ) {
        this.loc = loc;
        this.success = success;

        if (v != null) {
            if (v instanceof CacheObject)
                initValue(cctx, (CacheObject)v, keepBinary, ldr);
            else {
                assert loc;

                this.v = v;
            }
        }
    }

    /**
     * @return Value.
     */
    @Nullable public <V> V value() {
        return (V)v;
    }

    /**
     *
     */
    public boolean emptyResult() {
        return !invokeRes && v == null && cacheObj == null && success;
    }

    /**
     * @return If return is invoke result.
     */
    public boolean invokeResult() {
        return invokeRes;
    }

    /**
     * @param invokeRes Invoke result flag.
     */
    public void invokeResult(boolean invokeRes) {
        this.invokeRes = invokeRes;
    }

    /**
     * @param cctx Cache context.
     * @param v Value.
     * @param keepBinary Keep binary flag.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @return This instance for chaining.
     */
    public GridCacheReturn value(GridCacheContext cctx, CacheObject v, boolean keepBinary, @Nullable ClassLoader ldr) {
        initValue(cctx, v, keepBinary, ldr);

        return this;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return success;
    }

    /**
     * @param cctx Cache context.
     * @param cacheObj Value to set.
     * @param success Success flag to set.
     * @param keepBinary Keep binary flag.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @return This instance for chaining.
     */
    public GridCacheReturn set(
        GridCacheContext cctx,
        @Nullable CacheObject cacheObj,
        boolean success,
        boolean keepBinary,
        @Nullable ClassLoader ldr
    ) {
        this.success = success;

        initValue(cctx, cacheObj, keepBinary, ldr);

        return this;
    }

    /**
     * @param cctx Cache context.
     * @param cacheObj Cache object.
     * @param keepBinary Keep binary flag.
     * @param ldr Class loader, used for deserialization from binary representation.
     */
    private void initValue(
        GridCacheContext cctx,
        @Nullable CacheObject cacheObj,
        boolean keepBinary,
        @Nullable ClassLoader ldr
    ) {
        if (loc)
            v = cctx.cacheObjectContext().unwrapBinaryIfNeeded(cacheObj, keepBinary, true, ldr);
        else {
            assert cacheId == 0 || cacheId == cctx.cacheId();

            cacheId = cctx.cacheId();

            this.cacheObj = cacheObj;
        }
    }

    /**
     * @param success Success flag.
     * @return This instance for chaining.
     */
    public GridCacheReturn success(boolean success) {
        this.success = success;

        return this;
    }

    /**
     * @param cctx Context.
     * @param key Key.
     * @param key0 Key value.
     * @param res Result.
     * @param err Error.
     * @param keepBinary Keep binary.
     */
    public synchronized void addEntryProcessResult(
        GridCacheContext cctx,
        KeyCacheObject key,
        @Nullable Object key0,
        @Nullable Object res,
        @Nullable Exception err,
        boolean keepBinary) {
        assert v == null || v instanceof Map : v;
        assert key != null;
        assert res != null || err != null;

        invokeRes = true;

        if (loc) {
            HashMap<Object, EntryProcessorResult> resMap = (HashMap<Object, EntryProcessorResult>)v;

            if (resMap == null) {
                resMap = new HashMap<>();

                v = resMap;
            }

            // These exceptions mean that we should register class and call EntryProcessor again.
            if (err != null) {
                if (err instanceof UnregisteredClassException)
                    throw (UnregisteredClassException)err;
                else if (err instanceof UnregisteredBinaryTypeException)
                    throw (UnregisteredBinaryTypeException)err;
            }

            CacheInvokeResult res0 = err == null ? CacheInvokeResult.fromResult(res) : CacheInvokeResult.fromError(err);

            Object resKey = key0 != null ? key0 :
                ((keepBinary && key instanceof BinaryObject) ? key : CU.value(key, cctx, true));

            resMap.put(resKey, res0);
        }
        else {
            assert v == null;
            assert cacheId == 0 || cacheId == cctx.cacheId();

            cacheId = cctx.cacheId();

            if (invokeResCol == null)
                invokeResCol = new ArrayList<>();

            CacheInvokeDirectResult res0 = err == null ?
                cctx.transactional() ?
                    new CacheInvokeDirectResult(key, cctx.toCacheObject(res)) :
                    CacheInvokeDirectResult.lazyResult(key, res) :
                new CacheInvokeDirectResult(key, err);

            invokeResCol.add(res0);
        }
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @param other Other result to merge with.
     */
    public synchronized void mergeEntryProcessResults(GridCacheReturn other) {
        assert invokeRes || v == null : "Invalid state to merge: " + this;
        assert other.invokeRes;
        assert loc == other.loc : loc;

        if (other.v == null)
            return;

        invokeRes = true;

        HashMap<Object, EntryProcessorResult> resMap = (HashMap<Object, EntryProcessorResult>)v;

        if (resMap == null) {
            resMap = new HashMap<>();

            v = resMap;
        }

        resMap.putAll((Map<Object, EntryProcessorResult>)other.v);
    }

    /**
     * Converts entry processor invokation results to cache object instances.
     *
     * @param ctx Cache context.
     */
    public void marshalResult(GridCacheContext ctx) {
        if (invokeRes && invokeResCol != null) {
            for (CacheInvokeDirectResult directRes : invokeResCol)
                directRes.marshalResult(ctx);
        }
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        assert !loc;

        if (cacheObj != null)
            cacheObj.prepareMarshal(ctx.cacheObjectContext());

        if (invokeRes && invokeResCol != null) {
            for (CacheInvokeDirectResult res : invokeResCol)
                res.prepareMarshal(ctx);
        }
    }

    /**
     * @param ctx Cache context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        loc = true;

        if (cacheObj != null) {
            cacheObj.finishUnmarshal(ctx.cacheObjectContext(), ldr);

            v = ctx.cacheObjectContext().unwrapBinaryIfNeeded(cacheObj, true, false, ldr);
        }

        if (invokeRes && invokeResCol != null) {
            for (CacheInvokeDirectResult res : invokeResCol)
                res.finishUnmarshal(ctx, ldr);

            Map<Object, CacheInvokeResult> map0 = U.newHashMap(invokeResCol.size());

            for (CacheInvokeDirectResult res : invokeResCol) {
                CacheInvokeResult<?> res0 = res.error() == null ?
                    CacheInvokeResult.fromResult(ctx.cacheObjectContext().unwrapBinaryIfNeeded(res.result(), true, false, null)) :
                    CacheInvokeResult.fromError(res.error());

                map0.put(ctx.cacheObjectContext().unwrapBinaryIfNeeded(res.key(), true, false, null), res0);
            }

            v = map0;
        }
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 88;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt(cacheId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage(cacheObj))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean(invokeRes))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection(invokeResCol, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeBoolean(success))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                cacheId = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                cacheObj = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                invokeRes = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                invokeResCol = reader.readCollection(MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                success = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheReturn.class, this);
    }
}
