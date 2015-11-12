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

import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.portable.BinaryMetadata;
import org.apache.ignite.internal.portable.BinaryObjectImpl;
import org.apache.ignite.internal.portable.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.portable.BinaryWriterExImpl;
import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.internal.portable.PortableSchema;
import org.apache.ignite.internal.portable.PortableSchemaRegistry;
import org.apache.ignite.internal.portable.PortableUtils;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.ignite.internal.portable.GridPortableMarshaller.DFLT_HDR_LEN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLAGS_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.HASH_CODE_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PROTO_VER_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TYPE_ID_POS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UNREGISTERED_TYPE_ID;

/**
 *
 */
public class BinaryObjectBuilderImpl implements BinaryObjectBuilder {
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

    /** Flags. */
    private final short flags;

    /** Total header length */
    private final int hdrLen;

    /** Context of PortableObject reading process. Or {@code null} if object is not created from PortableObject. */
    private final PortableBuilderReader reader;

    /** */
    private int hashCode;

    /**
     * @param clsName Class name.
     * @param ctx Portable context.
     */
    public BinaryObjectBuilderImpl(PortableContext ctx, String clsName) {
        this(ctx, ctx.typeId(clsName), PortableContext.typeName(clsName));
    }

    /**
     * @param typeName Type name.
     * @param ctx Context.
     * @param typeId Type id.
     */
    public BinaryObjectBuilderImpl(PortableContext ctx, int typeId, String typeName) {
        this.typeId = typeId;
        this.typeName = typeName;
        this.ctx = ctx;

        start = -1;
        flags = -1;
        reader = null;
        hdrLen = DFLT_HDR_LEN;

        readCache = Collections.emptyMap();
    }

    /**
     * @param obj Object to wrap.
     */
    public BinaryObjectBuilderImpl(BinaryObjectImpl obj) {
        this(new PortableBuilderReader(obj), obj.start());

        reader.registerObject(this);
    }

    /**
     * @param reader ctx
     * @param start Start.
     */
    BinaryObjectBuilderImpl(PortableBuilderReader reader, int start) {
        this.reader = reader;
        this.start = start;
        this.flags = reader.readShortPositioned(start + FLAGS_POS);

        byte ver = reader.readBytePositioned(start + PROTO_VER_POS);

        PortableUtils.checkProtocolVersion(ver);

        int typeId = reader.readIntPositioned(start + TYPE_ID_POS);
        ctx = reader.portableContext();
        hashCode = reader.readIntPositioned(start + HASH_CODE_POS);

        if (typeId == UNREGISTERED_TYPE_ID) {
            int mark = reader.position();

            reader.position(start + DFLT_HDR_LEN);

            clsNameToWrite = reader.readString();

            Class cls;

            try {
                // TODO: IGNITE-1272 - Is class loader needed here?
                cls = U.forName(clsNameToWrite, null);
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsNameToWrite, e);
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
    @Override public BinaryObject build() {
        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, typeId, false)) {
            PortableBuilderSerializer serializationCtx = new PortableBuilderSerializer();

            serializationCtx.registerObjectWriting(this, 0);

            serializeTo(writer, serializationCtx);

            byte[] arr = writer.array();

            return new BinaryObjectImpl(ctx, arr, 0);
        }
    }

    /**
     * @param writer Writer.
     * @param serializer Serializer.
     */
    void serializeTo(BinaryWriterExImpl writer, PortableBuilderSerializer serializer) {
        try {
            PortableUtils.writeHeader(writer,
                registeredType ? typeId : UNREGISTERED_TYPE_ID,
                hashCode,
                registeredType ? null : clsNameToWrite
            );

            Set<Integer> remainsFlds = null;

            if (reader != null) {
                PortableSchema schema = reader.schema(start);

                Map<Integer, Object> assignedFldsById;

                if (assignedVals != null) {
                    assignedFldsById = U.newHashMap(assignedVals.size());

                    for (Map.Entry<String, Object> entry : assignedVals.entrySet()) {
                        int fieldId = ctx.fieldId(typeId, entry.getKey());

                        assignedFldsById.put(fieldId, entry.getValue());
                    }

                    remainsFlds = assignedFldsById.keySet();
                }
                else
                    assignedFldsById = Collections.emptyMap();

                // Get footer details.
                int fieldIdLen = PortableUtils.fieldIdLength(flags);
                int fieldOffsetLen = PortableUtils.fieldOffsetLength(flags);

                IgniteBiTuple<Integer, Integer> footer = PortableUtils.footerAbsolute(reader, start);

                int footerPos = footer.get1();
                int footerEnd = footer.get2();

                // Get raw position.
                int rawPos = PortableUtils.rawOffsetAbsolute(reader, start);

                // Position reader on data.
                reader.position(start + hdrLen);

                int idx = 0;

                while (reader.position() < rawPos) {
                    int fieldId = schema.fieldId(idx++);
                    int fieldLen =
                        fieldPositionAndLength(footerPos, footerEnd, rawPos, fieldIdLen, fieldOffsetLen).get2();

                    int postPos = reader.position() + fieldLen; // Position where reader will be placed afterwards.

                    footerPos += fieldIdLen + fieldOffsetLen;

                    if (assignedFldsById.containsKey(fieldId)) {
                        Object assignedVal = assignedFldsById.remove(fieldId);

                        if (assignedVal != REMOVED_FIELD_MARKER) {
                            writer.writeFieldId(fieldId);

                            serializer.writeValue(writer, assignedVal);
                        }
                    }
                    else {
                        int type = fieldLen != 0 ? reader.readByte(0) : 0;

                        if (fieldLen != 0 && !PortableUtils.isPlainArrayType(type) && PortableUtils.isPlainType(type)) {
                            writer.writeFieldId(fieldId);

                            writer.write(reader.array(), reader.position(), fieldLen);
                        }
                        else {
                            writer.writeFieldId(fieldId);

                            Object val;

                            if (fieldLen == 0)
                                val = null;
                            else if (readCache == null) {
                                val = reader.parseValue();

                                assert reader.position() == postPos;
                            }
                            else
                                val = readCache.get(fieldId);

                            serializer.writeValue(writer, val);
                        }
                    }

                    reader.position(postPos);
                }
            }

            BinaryType meta = ctx.metadata(typeId);

            Map<String, Integer> fieldsMeta = null;

            if (assignedVals != null && (remainsFlds == null || !remainsFlds.isEmpty())) {
                for (Map.Entry<String, Object> entry : assignedVals.entrySet()) {
                    Object val = entry.getValue();

                    if (val == REMOVED_FIELD_MARKER)
                        continue;

                    String name = entry.getKey();

                    int fieldId = ctx.fieldId(typeId, name);

                    if (remainsFlds != null && !remainsFlds.contains(fieldId))
                        continue;

                    writer.writeFieldId(fieldId);

                    serializer.writeValue(writer, val);

                    String oldFldTypeName = meta == null ? null : meta.fieldTypeName(name);

                    int newFldTypeId;

                    if (val instanceof PortableValueWithType)
                        newFldTypeId = ((PortableValueWithType) val).typeId();
                    else
                        newFldTypeId = PortableUtils.typeByClass(val.getClass());

                    String newFldTypeName = PortableUtils.fieldTypeName(newFldTypeId);

                    if (oldFldTypeName == null) {
                        // It's a new field, we have to add it to metadata.
                        if (fieldsMeta == null)
                            fieldsMeta = new HashMap<>();

                        fieldsMeta.put(name, PortableUtils.fieldTypeId(newFldTypeName));
                    }
                    else {
                        String objTypeName = PortableUtils.fieldTypeName(GridPortableMarshaller.OBJ);

                        if (!objTypeName.equals(oldFldTypeName) && !oldFldTypeName.equals(newFldTypeName)) {
                            throw new BinaryObjectException(
                                "Wrong value has been set [" +
                                    "typeName=" + (typeName == null ? meta.typeName() : typeName) +
                                    ", fieldName=" + name +
                                    ", fieldType=" + oldFldTypeName +
                                    ", assignedValueType=" + newFldTypeName + ']'
                            );
                        }
                    }
                }
            }

            if (reader != null) {
                // Write raw data if any.
                int rawOff = PortableUtils.rawOffsetAbsolute(reader, start);
                int footerStart = PortableUtils.footerStartAbsolute(reader, start);

                if (rawOff < footerStart) {
                    writer.rawWriter();

                    writer.write(reader.array(), rawOff, footerStart - rawOff);
                }

                // Shift reader to the end of the object.
                reader.position(start + PortableUtils.length(reader, start));
            }

            writer.postWrite(true);

            // Update metadata if needed.
            int schemaId = writer.schemaId();

            PortableSchemaRegistry schemaReg = ctx.schemaRegistry(typeId);

            if (schemaReg.schema(schemaId) == null) {
                String typeName = this.typeName;

                if (typeName == null) {
                    assert meta != null;

                    typeName = meta.typeName();
                }

                PortableSchema curSchema = writer.currentSchema();

                ctx.updateMetadata(typeId, new BinaryMetadata(typeId, typeName, fieldsMeta,
                    ctx.affinityKeyFieldName(typeId), Collections.singleton(curSchema)));

                schemaReg.addSchema(curSchema.schemaId(), curSchema);
            }
        }
        finally {
            writer.popSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilderImpl hashCode(int hashCode) {
        this.hashCode = hashCode;

        return this;
    }

    /**
     * Get field position and length.
     *
     * @param footerPos Field position inside the footer (absolute).
     * @param footerEnd Footer end (absolute).
     * @param rawPos Raw data position (absolute).
     * @param fieldIdLen Field ID length.
     * @param fieldOffsetLen Field offset length.
     * @return Tuple with field position and length.
     */
    private IgniteBiTuple<Integer, Integer> fieldPositionAndLength(int footerPos, int footerEnd, int rawPos,
        int fieldIdLen, int fieldOffsetLen) {
        // Get field offset first.
        int fieldOffset = PortableUtils.fieldOffsetRelative(reader, footerPos + fieldIdLen, fieldOffsetLen);
        int fieldPos = start + fieldOffset;

        // Get field length.
        int fieldLen;

        if (footerPos + fieldIdLen + fieldOffsetLen == footerEnd)
            // This is the last field, compare to raw offset.
            fieldLen = rawPos - fieldPos;
        else {
            // Field is somewhere in the middle, get difference with the next offset.
            int nextFieldOffset = PortableUtils.fieldOffsetRelative(reader,
                footerPos + fieldIdLen + fieldOffsetLen + fieldIdLen, fieldOffsetLen);

            fieldLen = nextFieldOffset - fieldOffset;
        }

        return F.t(fieldPos, fieldLen);
    }

    /**
     * Initialize read cache if needed.
     */
    private void ensureReadCacheInit() {
        assert reader != null;

        if (readCache == null) {
            int fieldIdLen = PortableUtils.fieldIdLength(flags);
            int fieldOffsetLen = PortableUtils.fieldOffsetLength(flags);

            PortableSchema schema = reader.schema(start);

            Map<Integer, Object> readCache = new HashMap<>();

            IgniteBiTuple<Integer, Integer> footer = PortableUtils.footerAbsolute(reader, start);

            int footerPos = footer.get1();
            int footerEnd = footer.get2();

            int rawPos = PortableUtils.rawOffsetAbsolute(reader, start);

            int idx = 0;

            while (footerPos + fieldIdLen < footerEnd) {
                int fieldId = schema.fieldId(idx++);

                IgniteBiTuple<Integer, Integer> posAndLen =
                    fieldPositionAndLength(footerPos, footerEnd, rawPos, fieldIdLen, fieldOffsetLen);

                Object val = reader.getValueQuickly(posAndLen.get1(), posAndLen.get2());

                readCache.put(fieldId, val);

                // Shift current footer position.
                footerPos += fieldIdLen + fieldOffsetLen;
            }

            this.readCache = readCache;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T getField(String name) {
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

        return (T)PortableUtils.unwrapLazy(val);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder setField(String name, Object val) {
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
    @Override public <T> BinaryObjectBuilder setField(String name, @Nullable T val, Class<? super T> type) {
        if (assignedVals == null)
            assignedVals = new LinkedHashMap<>();

        //int fldId = ctx.fieldId(typeId, fldName);

        assignedVals.put(name, new PortableValueWithType(PortableUtils.typeByClass(type), val));

        return this;
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder setField(String name, @Nullable BinaryObjectBuilder builder) {
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
    @Override public BinaryObjectBuilderImpl removeField(String name) {
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
    public static BinaryObjectBuilderImpl wrap(BinaryObject obj) {
        BinaryObjectImpl heapObj;

        if (obj instanceof BinaryObjectOffheapImpl)
            heapObj = (BinaryObjectImpl)((BinaryObjectOffheapImpl)obj).heapCopy();
        else
            heapObj = (BinaryObjectImpl)obj;

        return new BinaryObjectBuilderImpl(heapObj);
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