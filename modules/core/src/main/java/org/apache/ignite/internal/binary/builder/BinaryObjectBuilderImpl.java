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

package org.apache.ignite.internal.binary.builder;

import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinarySchema;
import org.apache.ignite.internal.binary.BinarySchemaRegistry;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 */
public class BinaryObjectBuilderImpl implements BinaryObjectBuilder {
    /** */
    private static final Object REMOVED_FIELD_MARKER = new Object();

    /** */
    private final BinaryContext ctx;

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

    /** Position of object in source array, or -1 if object is not created from BinaryObject. */
    private final int start;

    /** Flags. */
    private final short flags;

    /** Total header length */
    private final int hdrLen;

    /** Context of BinaryObject reading process. Or {@code null} if object is not created from BinaryObject. */
    private final BinaryBuilderReader reader;

    /** */
    private int hashCode;

    /** */
    private boolean isHashCodeSet;

    /**
     * @param clsName Class name.
     * @param ctx Binary context.
     */
    public BinaryObjectBuilderImpl(BinaryContext ctx, String clsName) {
        this(ctx, ctx.typeId(clsName), ctx.userTypeName(clsName));
    }

    /**
     * @param typeName Type name.
     * @param ctx Context.
     * @param typeId Type id.
     */
    public BinaryObjectBuilderImpl(BinaryContext ctx, int typeId, String typeName) {
        this.typeId = typeId;
        this.typeName = typeName;
        this.ctx = ctx;

        start = -1;
        flags = -1;
        reader = null;
        hdrLen = GridBinaryMarshaller.DFLT_HDR_LEN;

        readCache = Collections.emptyMap();
    }

    /**
     * @param obj Object to wrap.
     */
    public BinaryObjectBuilderImpl(BinaryObjectImpl obj) {
        this(new BinaryBuilderReader(obj), obj.start());
        isHashCodeSet = !obj.isFlagSet(BinaryUtils.FLAG_EMPTY_HASH_CODE);
        reader.registerObject(this);
    }

    /**
     * @param reader ctx
     * @param start Start.
     */
    BinaryObjectBuilderImpl(BinaryBuilderReader reader, int start) {
        assert reader != null;

        this.reader = reader;
        this.start = start;
        this.flags = reader.readShortPositioned(start + GridBinaryMarshaller.FLAGS_POS);

        byte ver = reader.readBytePositioned(start + GridBinaryMarshaller.PROTO_VER_POS);

        BinaryUtils.checkProtocolVersion(ver);

        int typeId = reader.readIntPositioned(start + GridBinaryMarshaller.TYPE_ID_POS);
        ctx = reader.binaryContext();
        hashCode = reader.readIntPositioned(start + GridBinaryMarshaller.HASH_CODE_POS);

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            int mark = reader.position();

            reader.position(start + GridBinaryMarshaller.DFLT_HDR_LEN);

            clsNameToWrite = reader.readString();

            Class cls;

            try {
                cls = U.forName(clsNameToWrite, ctx.configuration().getClassLoader());
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsNameToWrite, e);
            }

            this.typeId = ctx.descriptorForClass(cls, false).typeId();

            registeredType = false;

            hdrLen = reader.position() - mark;

            reader.position(mark);
        }
        else {
            this.typeId = typeId;
            hdrLen = GridBinaryMarshaller.DFLT_HDR_LEN;
        }
    }

    /** {@inheritDoc} */
    @Override public BinaryObject build() {
        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx)) {
            writer.typeId(typeId);

            BinaryBuilderSerializer serializationCtx = new BinaryBuilderSerializer();

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
    @SuppressWarnings("ResultOfMethodCallIgnored")
    void serializeTo(BinaryWriterExImpl writer, BinaryBuilderSerializer serializer) {
        try {
            writer.preWrite(registeredType ? null : clsNameToWrite);

            Set<Integer> remainsFlds = null;

            BinaryType meta = ctx.metadata(typeId);

            Map<String, Integer> fieldsMeta = null;

            if (reader != null && BinaryUtils.hasSchema(flags)) {
                BinarySchema schema = reader.schema();

                Map<Integer, Object> assignedFldsById;

                if (assignedVals != null) {
                    assignedFldsById = U.newHashMap(assignedVals.size());

                    for (Map.Entry<String, Object> entry : assignedVals.entrySet()) {
                        String name = entry.getKey();
                        Object val = entry.getValue();

                        int fieldId = ctx.fieldId(typeId, name);

                        assignedFldsById.put(fieldId, val);

                        if (val != REMOVED_FIELD_MARKER)
                            fieldsMeta = checkMetadata(meta, fieldsMeta, val, name);
                    }

                    remainsFlds = assignedFldsById.keySet();
                }
                else
                    assignedFldsById = Collections.emptyMap();

                // Get footer details.
                int fieldIdLen = BinaryUtils.fieldIdLength(flags);
                int fieldOffsetLen = BinaryUtils.fieldOffsetLength(flags);

                IgniteBiTuple<Integer, Integer> footer = BinaryUtils.footerAbsolute(reader, start);

                int footerPos = footer.get1();
                int footerEnd = footer.get2();

                // Get raw position.
                int rawPos = BinaryUtils.rawOffsetAbsolute(reader, start);

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

                        if (fieldLen != 0 && !BinaryUtils.isPlainArrayType(type) && BinaryUtils.isPlainType(type)) {
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

                    if (reader == null)
                        // Metadata has already been checked.
                        fieldsMeta = checkMetadata(meta, fieldsMeta, val, name);
                }
            }

            if (reader != null) {
                // Write raw data if any.
                int rawOff = BinaryUtils.rawOffsetAbsolute(reader, start);
                int footerStart = BinaryUtils.footerStartAbsolute(reader, start);

                if (rawOff < footerStart) {
                    writer.rawWriter();

                    writer.write(reader.array(), rawOff, footerStart - rawOff);
                }

                // Shift reader to the end of the object.
                reader.position(start + BinaryUtils.length(reader, start));
            }

            //noinspection NumberEquality
            writer.postWrite(true, registeredType, hashCode, isHashCodeSet);

            // Update metadata if needed.
            int schemaId = writer.schemaId();

            BinarySchemaRegistry schemaReg = ctx.schemaRegistry(typeId);

            if (schemaReg.schema(schemaId) == null) {
                String typeName = this.typeName;

                if (typeName == null) {
                    assert meta != null;

                    typeName = meta.typeName();
                }

                BinarySchema curSchema = writer.currentSchema();

                ctx.updateMetadata(typeId, new BinaryMetadata(typeId, typeName, fieldsMeta,
                    ctx.affinityKeyFieldName(typeId), Collections.singleton(curSchema), false));

                schemaReg.addSchema(curSchema.schemaId(), curSchema);
            }

            // Update hash code after schema is written.
            writer.postWriteHashCode(registeredType ? null : clsNameToWrite);
        }
        finally {
            writer.popSchema();
        }
    }

    /**
     * Checks metadata when a BinaryObject is being serialized.
     *
     * @param meta Current metadata.
     * @param fieldsMeta Map holding metadata information that has to be updated.
     * @param newVal Field value being serialized.
     * @param name Field name.
     */
    private Map<String, Integer> checkMetadata(BinaryType meta, Map<String, Integer> fieldsMeta, Object newVal,
        String name) {
        String oldFldTypeName = meta == null ? null : meta.fieldTypeName(name);

        boolean nullFieldVal = false;

        int newFldTypeId;

        if (newVal instanceof BinaryValueWithType) {
            newFldTypeId = ((BinaryValueWithType)newVal).typeId();

            if (((BinaryValueWithType)newVal).value() == null)
                nullFieldVal = true;
        }
        // Detect Enum and Enum array type.
        else if (newVal instanceof BinaryEnumObjectImpl)
            newFldTypeId = GridBinaryMarshaller.ENUM;
        else if (newVal.getClass().isArray() && newVal.getClass().getComponentType() == BinaryObject.class) {
            BinaryObject[] arr = (BinaryObject[])newVal;

            newFldTypeId = arr.length > 0 && arr[0] instanceof BinaryEnumObjectImpl ?
                GridBinaryMarshaller.ENUM_ARR : GridBinaryMarshaller.OBJ_ARR;
        }
        else
            newFldTypeId = BinaryUtils.typeByClass(newVal.getClass());

        if (oldFldTypeName == null) {
            // It's a new field, we have to add it to metadata.
            if (fieldsMeta == null) {
                if (BinaryUtils.FIELDS_SORTED_ORDER)
                    fieldsMeta = new TreeMap<>();
                else
                    fieldsMeta = new LinkedHashMap<>();
            }

            fieldsMeta.put(name, newFldTypeId);
        }
        else if (!nullFieldVal) {
            String newFldTypeName = BinaryUtils.fieldTypeName(newFldTypeId);

            if (!F.eq(newFldTypeName, oldFldTypeName)) {
                throw new BinaryObjectException(
                    "Wrong value has been set [" +
                        "typeName=" + (typeName == null ? meta.typeName() : typeName) +
                        ", fieldName=" + name +
                        ", fieldType=" + oldFldTypeName +
                        ", assignedValueType=" + newFldTypeName + ']'
                );
            }
        }

        return fieldsMeta;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("UnnecessaryBoxing")
    @Override public BinaryObjectBuilderImpl hashCode(int hashCode) {
        this.hashCode = hashCode;

        isHashCodeSet = true;

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
        int fieldOffset = BinaryUtils.fieldOffsetRelative(reader, footerPos + fieldIdLen, fieldOffsetLen);
        int fieldPos = start + fieldOffset;

        // Get field length.
        int fieldLen;

        if (footerPos + fieldIdLen + fieldOffsetLen == footerEnd)
            // This is the last field, compare to raw offset.
            fieldLen = rawPos - fieldPos;
        else {
            // Field is somewhere in the middle, get difference with the next offset.
            int nextFieldOffset = BinaryUtils.fieldOffsetRelative(reader,
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
            int fieldIdLen = BinaryUtils.fieldIdLength(flags);
            int fieldOffsetLen = BinaryUtils.fieldOffsetLength(flags);

            BinarySchema schema = reader.schema();

            Map<Integer, Object> readCache = new HashMap<>();

            IgniteBiTuple<Integer, Integer> footer = BinaryUtils.footerAbsolute(reader, start);

            int footerPos = footer.get1();
            int footerEnd = footer.get2();

            int rawPos = BinaryUtils.rawOffsetAbsolute(reader, start);

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

        return (T)BinaryUtils.unwrapLazy(val);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder setField(String name, Object val0) {
        Object val = val0 == null ? new BinaryValueWithType(BinaryUtils.typeByClass(Object.class), null) : val0;

        if (assignedVals == null) {
            if (BinaryUtils.FIELDS_SORTED_ORDER)
                assignedVals = new TreeMap<>();
            else
                assignedVals = new LinkedHashMap<>();
        }

        Object oldVal = assignedVals.put(name, val);

        if (oldVal instanceof BinaryValueWithType && val0 != null) {
            ((BinaryValueWithType)oldVal).value(val);

            assignedVals.put(name, oldVal);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> BinaryObjectBuilder setField(String name, @Nullable T val, Class<? super T> type) {
        byte typeId;

        if (Collection.class.equals(type))
            typeId = GridBinaryMarshaller.COL;
        else if (Map.class.equals(type))
            typeId = GridBinaryMarshaller.MAP;
        else
            typeId = BinaryUtils.typeByClass(type);

        if (assignedVals == null)
            assignedVals = new LinkedHashMap<>();

        assignedVals.put(name, new BinaryValueWithType(typeId, val));

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
     * Removes field from binary object.
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
     * Creates builder initialized by specified binary object.
     *
     * @param obj Binary object to initialize builder.
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
