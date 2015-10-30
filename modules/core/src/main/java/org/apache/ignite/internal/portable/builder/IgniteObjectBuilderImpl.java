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

package org.apache.ignite.internal.portable.builder;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.portable.CacheObjectPortableProcessorImpl;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.igniteobject.IgniteObjectBuilder;
import org.apache.ignite.igniteobject.IgniteObjectException;
import org.apache.ignite.igniteobject.IgniteObjectInvalidClassException;
import org.apache.ignite.igniteobject.IgniteObjectMetadata;
import org.apache.ignite.igniteobject.IgniteObject;
import org.jetbrains.annotations.Nullable;
import org.apache.ignite.internal.portable.*;

import static org.apache.ignite.internal.portable.GridPortableMarshaller.CLS_NAME_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DFLT_HDR_LEN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.HASH_CODE_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PROTO_VER;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PROTO_VER_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.RAW_DATA_OFF_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TOTAL_LEN_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TYPE_ID_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UNREGISTERED_TYPE_ID;

/**
 *
 */
public class IgniteObjectBuilderImpl implements IgniteObjectBuilder {
    /** */
    private static final Object REMOVED_FIELD_MARKER = new Object();

    /** */
    private final PortableContext ctx;

    /** */
    private final int typeId;

    /** May be null. */
    private String typeName;

    /** May be null. */
    private String clsNameToWrite;

    /** */
    private boolean registeredType = true;

    /** */
    private Map<String, Object> assignedVals;

    /** */
    private Map<Integer, Object> readCache;

    /** Position of object in source array, or -1 if object is not created from PortableObject. */
    private final int start;

    /** Total header length */
    private final int hdrLen;

    /**
     * Context of PortableObject reading process. Or {@code null} if object is not created from PortableObject.
     */
    private final PortableBuilderReader reader;

    /** */
    private int hashCode;

    /**
     * @param clsName Class name.
     * @param ctx Portable context.
     */
    public IgniteObjectBuilderImpl(PortableContext ctx, String clsName) {
        this(ctx, ctx.typeId(clsName), PortableContext.typeName(clsName));
    }

    /**
     * @param typeId Type ID.
     * @param ctx Portable context.
     */
    public IgniteObjectBuilderImpl(PortableContext ctx, int typeId) {
        this(ctx, typeId, null);
    }

    /**
     * @param typeName Type name.
     * @param ctx Context.
     * @param typeId Type id.
     */
    public IgniteObjectBuilderImpl(PortableContext ctx, int typeId, String typeName) {
        this.typeId = typeId;
        this.typeName = typeName;
        this.ctx = ctx;

        start = -1;
        reader = null;
        hdrLen = DFLT_HDR_LEN;

        readCache = Collections.emptyMap();
    }

    /**
     * @param obj Object to wrap.
     */
    public IgniteObjectBuilderImpl(IgniteObjectImpl obj) {
        this(new PortableBuilderReader(obj), obj.start());

        reader.registerObject(this);
    }

    /**
     * @param reader ctx
     * @param start Start.
     */
    IgniteObjectBuilderImpl(PortableBuilderReader reader, int start) {
        this.reader = reader;
        this.start = start;

        byte ver = reader.readByteAbsolute(start + PROTO_VER_POS);

        PortableUtils.checkProtocolVersion(ver);

        int typeId = reader.readIntAbsolute(start + TYPE_ID_POS);
        ctx = reader.portableContext();
        hashCode = reader.readIntAbsolute(start + HASH_CODE_POS);

        if (typeId == UNREGISTERED_TYPE_ID) {
            int mark = reader.position();

            reader.position(start + CLS_NAME_POS);

            clsNameToWrite = reader.readString();

            Class cls;

            try {
                // TODO: IGNITE-1272 - Is class loader needed here?
                cls = U.forName(clsNameToWrite, null);
            }
            catch (ClassNotFoundException e) {
                throw new IgniteObjectInvalidClassException("Failed to load the class: " + clsNameToWrite, e);
            }

            this.typeId = ctx.descriptorForClass(cls).typeId();

            registeredType = false;

            hdrLen = reader.position() - mark;

            reader.position(mark);
        }
        else {
            this.typeId = typeId;
            hdrLen = DFLT_HDR_LEN;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteObject build() {
        try (IgniteObjectWriterExImpl writer = new IgniteObjectWriterExImpl(ctx, 0, typeId, false)) {

            PortableBuilderSerializer serializationCtx = new PortableBuilderSerializer();

            serializationCtx.registerObjectWriting(this, 0);

            serializeTo(writer, serializationCtx);

            byte[] arr = writer.array();

            return new IgniteObjectImpl(ctx, arr, 0);
        }
    }

    /**
     * @param writer Writer.
     * @param serializer Serializer.
     */
    void serializeTo(IgniteObjectWriterExImpl writer, PortableBuilderSerializer serializer) {
        writer.doWriteByte(GridPortableMarshaller.OBJ);
        writer.doWriteByte(PROTO_VER);
        writer.doWriteBoolean(true);
        writer.doWriteInt(registeredType ? typeId : UNREGISTERED_TYPE_ID);
        writer.doWriteInt(hashCode);

        // Length and raw offset.
        writer.reserve(8);

        if (!registeredType)
            writer.writeString(clsNameToWrite);

        Set<Integer> remainsFlds = null;

        if (reader != null) {
            Map<Integer, Object> assignedFldsById;

            if (assignedVals != null) {
                assignedFldsById = U.newHashMap(assignedVals.size());

                for (Map.Entry<String, Object> entry : assignedVals.entrySet()) {
                    int fldId = ctx.fieldId(typeId, entry.getKey());

                    assignedFldsById.put(fldId, entry.getValue());
                }

                remainsFlds = assignedFldsById.keySet();
            }
            else
                assignedFldsById = Collections.emptyMap();

            int rawOff = start + reader.readIntAbsolute(start + RAW_DATA_OFF_POS);

            reader.position(start + hdrLen);

            int cpStart = -1;

            while (reader.position() < rawOff) {
                int fldId = reader.readInt();

                int len = reader.readInt();

                if (assignedFldsById.containsKey(fldId)) {
                    if (cpStart >= 0) {
                        writer.write(reader.array(), cpStart, reader.position() - 4 - 4 - cpStart);

                        cpStart = -1;
                    }

                    Object assignedVal = assignedFldsById.remove(fldId);

                    reader.skip(len);

                    if (assignedVal != REMOVED_FIELD_MARKER) {
                        writer.writeInt(fldId);

                        int lenPos = writer.reserveAndMark(4);

                        serializer.writeValue(writer, assignedVal);

                        writer.writeDelta(lenPos);
                    }
                }
                else {
                    int type = len != 0 ? reader.readByte(0) : 0;

                    if (len != 0 && !PortableUtils.isPlainArrayType(type) && PortableUtils.isPlainType(type)) {
                        if (cpStart < 0)
                            cpStart = reader.position() - 4 - 4;

                        reader.skip(len);
                    }
                    else {
                        if (cpStart >= 0) {
                            writer.write(reader.array(), cpStart, reader.position() - 4 - cpStart);

                            cpStart = -1;
                        }
                        else
                            writer.writeInt(fldId);

                        Object val;

                        if (len == 0)
                            val = null;
                        else if (readCache == null) {
                            int savedPos = reader.position();

                            val = reader.parseValue();

                            assert reader.position() == savedPos + len;
                        }
                        else {
                            val = readCache.get(fldId);

                            reader.skip(len);
                        }

                        int lenPos = writer.reserveAndMark(4);

                        serializer.writeValue(writer, val);

                        writer.writeDelta(lenPos);
                    }
                }
            }

            if (cpStart >= 0)
                writer.write(reader.array(), cpStart, reader.position() - cpStart);
        }

        if (assignedVals != null && (remainsFlds == null || !remainsFlds.isEmpty())) {
            boolean metadataEnabled = ctx.isMetaDataEnabled(typeId);

            IgniteObjectMetadata metadata = null;

            if (metadataEnabled)
                metadata = ctx.metaData(typeId);

            Map<String, String> newFldsMetadata = null;

            for (Map.Entry<String, Object> entry : assignedVals.entrySet()) {
                Object val = entry.getValue();

                if (val == REMOVED_FIELD_MARKER)
                    continue;

                String name = entry.getKey();

                int fldId = ctx.fieldId(typeId, name);

                if (remainsFlds != null && !remainsFlds.contains(fldId))
                    continue;

                writer.writeInt(fldId);

                int lenPos = writer.reserveAndMark(4);

                serializer.writeValue(writer, val);

                writer.writeDelta(lenPos);

                if (metadataEnabled) {
                    String oldFldTypeName = metadata == null ? null : metadata.fieldTypeName(name);

                    String newFldTypeName;

                    if (val instanceof PortableValueWithType)
                        newFldTypeName = ((PortableValueWithType)val).typeName();
                    else {
                        byte type = PortableUtils.typeByClass(val.getClass());

                        newFldTypeName = CacheObjectPortableProcessorImpl.fieldTypeName(type);
                    }

                    if (oldFldTypeName == null) {
                        // It's a new field, we have to add it to metadata.

                        if (newFldsMetadata == null)
                            newFldsMetadata = new HashMap<>();

                        newFldsMetadata.put(name, newFldTypeName);
                    }
                    else {
                        if (!"Object".equals(oldFldTypeName) && !oldFldTypeName.equals(newFldTypeName)) {
                            throw new IgniteObjectException(
                                "Wrong value has been set [" +
                                    "typeName=" + (typeName == null ? metadata.typeName() : typeName) +
                                    ", fieldName=" + name +
                                    ", fieldType=" + oldFldTypeName +
                                    ", assignedValueType=" + newFldTypeName +
                                    ", assignedValue=" + (((PortableValueWithType)val).value()) + ']'
                            );
                        }
                    }
                }
            }

            if (newFldsMetadata != null) {
                String typeName = this.typeName;

                if (typeName == null)
                    typeName = metadata.typeName();

                ctx.updateMetaData(typeId, typeName, newFldsMetadata);
            }
        }

        writer.writeRawOffsetIfNeeded();

        if (reader != null) {
            int rawOff = reader.readIntAbsolute(start + RAW_DATA_OFF_POS);
            int len = reader.readIntAbsolute(start + TOTAL_LEN_POS);

            if (rawOff < len)
                writer.write(reader.array(), rawOff, len - rawOff);
        }

        writer.writeLength();
    }

    /** {@inheritDoc} */
    @Override public IgniteObjectBuilderImpl hashCode(int hashCode) {
        this.hashCode = hashCode;

        return this;
    }

    /**
     *
     */
    private void ensureReadCacheInit() {
        if (readCache == null) {
            Map<Integer, Object> readCache = new HashMap<>();

            int pos = start + hdrLen;
            int end = start + reader.readIntAbsolute(start + RAW_DATA_OFF_POS);

            while (pos < end) {
                int fieldId = reader.readIntAbsolute(pos);

                pos += 4;

                int len = reader.readIntAbsolute(pos);

                pos += 4;

                Object val = reader.getValueQuickly(pos, len);

                readCache.put(fieldId, val);

                pos += len;
            }

            this.readCache = readCache;
        }
    }

    /** {@inheritDoc} */
    @Override public <F> F getField(String name) {
        Object val;

        if (assignedVals != null && assignedVals.containsKey(name)) {
            val = assignedVals.get(name);

            if (val == REMOVED_FIELD_MARKER)
                return null;
        }
        else {
            ensureReadCacheInit();

            int fldId = ctx.fieldId(typeId, name);

            val = readCache.get(fldId);
        }

        return (F)PortableUtils.unwrapLazy(val);
    }

    /** {@inheritDoc} */
    @Override public IgniteObjectBuilder setField(String name, Object val) {
        GridArgumentCheck.notNull(val, name);

        if (assignedVals == null)
            assignedVals = new LinkedHashMap<>();

        Object oldVal = assignedVals.put(name, val);

        if (oldVal instanceof PortableValueWithType) {
            ((PortableValueWithType)oldVal).value(val);

            assignedVals.put(name, oldVal);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteObjectBuilder setField(String name, @Nullable T val, Class<? super T> type) {
        if (assignedVals == null)
            assignedVals = new LinkedHashMap<>();

        //int fldId = ctx.fieldId(typeId, fldName);

        assignedVals.put(name, new PortableValueWithType(PortableUtils.typeByClass(type), val));

        return this;
    }

    /** {@inheritDoc} */
    @Override public IgniteObjectBuilder setField(String name, @Nullable IgniteObjectBuilder builder) {
        if (builder == null)
            return setField(name, null, Object.class);
        else
            return setField(name, (Object)builder);
    }

    /**
     * Removes field from portable object.
     *
     * @param name Field name.
     * @return {@code this} instance for chaining.
     */
    @Override public IgniteObjectBuilderImpl removeField(String name) {
        if (assignedVals == null)
            assignedVals = new LinkedHashMap<>();

        assignedVals.put(name, REMOVED_FIELD_MARKER);

        return this;
    }

    /**
     * Creates builder initialized by specified portable object.
     *
     * @param obj Portable object to initialize builder.
     * @return New builder.
     */
    public static IgniteObjectBuilderImpl wrap(IgniteObject obj) {
        IgniteObjectImpl heapObj;

        if (obj instanceof IgniteObjectOffheapImpl)
            heapObj = (IgniteObjectImpl)((IgniteObjectOffheapImpl)obj).heapCopy();
        else
            heapObj = (IgniteObjectImpl)obj;

        return new IgniteObjectBuilderImpl(heapObj);
    }

    /**
     * @return Object start position in source array.
     */
    int start() {
        return start;
    }

    /**
     * @return Object type id.
     */
    public int typeId() {
        return typeId;
    }
}