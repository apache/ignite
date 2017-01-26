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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Binary.Metadata;

    /// <summary>
    /// Binary builder implementation.
    /// </summary>
    internal class BinaryObjectBuilder : IBinaryObjectBuilder
    {
        /** Cached dictionary with no values. */
        private static readonly IDictionary<int, BinaryBuilderField> EmptyVals =
            new Dictionary<int, BinaryBuilderField>();
        
        /** Binary. */
        private readonly Binary _binary;

        /** */
        private readonly BinaryObjectBuilder _parent;

        /** Initial binary object. */
        private readonly BinaryObject _obj;

        /** Type descriptor. */
        private readonly IBinaryTypeDescriptor _desc;

        /** Values. */
        private IDictionary<string, BinaryBuilderField> _vals;

        /** Contextual fields. */
        private IDictionary<int, BinaryBuilderField> _cache;

        /** Hash code. */
        private int? _hashCode;
        
        /** Current context. */
        private Context _ctx;

        /** Write array action. */
        private static readonly Action<BinaryWriter, object> WriteArrayAction = 
            (w, o) => w.WriteArrayInternal((Array) o);

        /** Write collection action. */
        private static readonly Action<BinaryWriter, object> WriteCollectionAction = 
            (w, o) => w.WriteCollection((ICollection) o);

        /** Write timestamp action. */
        private static readonly Action<BinaryWriter, object> WriteTimestampAction = 
            (w, o) => w.WriteTimestamp((DateTime?) o);

        /** Write timestamp array action. */
        private static readonly Action<BinaryWriter, object> WriteTimestampArrayAction = 
            (w, o) => w.WriteTimestampArray((DateTime?[])o);

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="binary">Binary.</param>
        /// <param name="parent">Parent builder.</param>
        /// <param name="obj">Initial binary object.</param>
        /// <param name="desc">Type descriptor.</param>
        public BinaryObjectBuilder(Binary binary, BinaryObjectBuilder parent, 
            BinaryObject obj, IBinaryTypeDescriptor desc)
        {
            Debug.Assert(binary != null);
            Debug.Assert(desc != null);

            _binary = binary;
            _parent = parent ?? this;
            _desc = desc;

            if (obj != null)
            {
                _obj = obj;
                _hashCode = obj.GetHashCode();
            }
            else
            {
                _obj = BinaryFromDescriptor(desc);
            }
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetHashCode(int hashCode)
        {
            _hashCode = hashCode;

            return this;
        }

        /** <inheritDoc /> */
        public T GetField<T>(string name)
        {
            BinaryBuilderField field;

            if (_vals != null && _vals.TryGetValue(name, out field))
                return field != BinaryBuilderField.RmvMarker ? (T) field.Value : default(T);

            int pos;

            if (!_obj.TryGetFieldPosition(name, out pos))
                return default(T);

            T val;

            if (TryGetCachedField(pos, out val))
                return val;

            val = _obj.GetField<T>(pos, this);

            var fld = CacheField(pos, val);

            SetField0(name, fld);

            return val;
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetField<T>(string fieldName, T val)
        {
            return SetField0(fieldName,
                new BinaryBuilderField(typeof (T), val, BinarySystemHandlers.GetTypeId(typeof (T))));
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetArrayField<T>(string fieldName, T[] val)
        {
            return SetField0(fieldName,
                new BinaryBuilderField(typeof (T[]), val, BinaryUtils.TypeArray, WriteArrayAction));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetBooleanField(string fieldName, bool val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (bool), val, BinaryUtils.TypeBool, 
                (w, o) => w.WriteBooleanField((bool) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetBooleanArrayField(string fieldName, bool[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (bool[]), val, BinaryUtils.TypeArrayBool,
                (w, o) => w.WriteBooleanArray((bool[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetByteField(string fieldName, byte val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (byte), val, BinaryUtils.TypeByte,
                (w, o) => w.WriteByteField((byte) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetByteArrayField(string fieldName, byte[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (byte[]), val, BinaryUtils.TypeArrayByte,
                (w, o) => w.WriteByteArray((byte[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetCharField(string fieldName, char val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (char), val, BinaryUtils.TypeChar,
                (w, o) => w.WriteCharField((char) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetCharArrayField(string fieldName, char[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (char[]), val, BinaryUtils.TypeArrayChar,
                (w, o) => w.WriteCharArray((char[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetCollectionField(string fieldName, ICollection val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (ICollection), val, BinaryUtils.TypeCollection,
                WriteCollectionAction));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetDecimalField(string fieldName, decimal? val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (decimal?), val, BinaryUtils.TypeDecimal,
                (w, o) => w.WriteDecimal((decimal?) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetDecimalArrayField(string fieldName, decimal?[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (decimal?[]), val, BinaryUtils.TypeArrayDecimal,
                (w, o) => w.WriteDecimalArray((decimal?[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetDictionaryField(string fieldName, IDictionary val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (IDictionary), val, BinaryUtils.TypeDictionary,
                (w, o) => w.WriteDictionary((IDictionary) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetDoubleField(string fieldName, double val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (double), val, BinaryUtils.TypeDouble,
                (w, o) => w.WriteDoubleField((double) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetDoubleArrayField(string fieldName, double[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (double[]), val, BinaryUtils.TypeArrayDouble,
                (w, o) => w.WriteDoubleArray((double[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetEnumField<T>(string fieldName, T val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (T), val, BinaryUtils.TypeEnum,
                (w, o) => w.WriteEnum((T) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetEnumArrayField<T>(string fieldName, T[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (T[]), val, BinaryUtils.TypeArrayEnum,
                (w, o) => w.WriteEnumArray((T[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetFloatField(string fieldName, float val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (float), val, BinaryUtils.TypeFloat,
                (w, o) => w.WriteFloatField((float) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetFloatArrayField(string fieldName, float[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (float[]), val, BinaryUtils.TypeArrayFloat,
                (w, o) => w.WriteFloatArray((float[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetGuidField(string fieldName, Guid? val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (Guid?), val, BinaryUtils.TypeGuid,
                (w, o) => w.WriteGuid((Guid?) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetGuidArrayField(string fieldName, Guid?[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (Guid?[]), val, BinaryUtils.TypeArrayGuid,
                (w, o) => w.WriteGuidArray((Guid?[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetIntField(string fieldName, int val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (int), val, BinaryUtils.TypeInt,
                (w, o) => w.WriteIntField((int) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetIntArrayField(string fieldName, int[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (int[]), val, BinaryUtils.TypeArrayInt,
                (w, o) => w.WriteIntArray((int[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetLongField(string fieldName, long val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (long), val, BinaryUtils.TypeLong,
                (w, o) => w.WriteLongField((long) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetLongArrayField(string fieldName, long[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (long[]), val, BinaryUtils.TypeArrayLong,
                (w, o) => w.WriteLongArray((long[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetShortField(string fieldName, short val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (short), val, BinaryUtils.TypeShort,
                (w, o) => w.WriteShortField((short) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetShortArrayField(string fieldName, short[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (short[]), val, BinaryUtils.TypeArrayShort,
                (w, o) => w.WriteShortArray((short[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetStringField(string fieldName, string val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (string), val, BinaryUtils.TypeString,
                (w, o) => w.WriteString((string) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetStringArrayField(string fieldName, string[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (string[]), val, BinaryUtils.TypeArrayString,
                (w, o) => w.WriteStringArray((string[]) o)));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetTimestampField(string fieldName, DateTime? val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (DateTime?), val, BinaryUtils.TypeTimestamp,
                WriteTimestampAction));
        }
 
        /** <inheritDoc /> */
        public IBinaryObjectBuilder SetTimestampArrayField(string fieldName, DateTime?[] val)
        {
            return SetField0(fieldName, new BinaryBuilderField(typeof (DateTime?[]), val, BinaryUtils.TypeArrayTimestamp,
                WriteTimestampArrayAction));
        } 

        /** <inheritDoc /> */
        public IBinaryObjectBuilder RemoveField(string name)
        {
            return SetField0(name, BinaryBuilderField.RmvMarker);
        }

        /** <inheritDoc /> */
        public IBinaryObject Build()
        {
            // Assume that resulting length will be no less than header + [fields_cnt] * 12;
            int estimatedCapacity = BinaryObjectHeader.Size + (_vals == null ? 0 : _vals.Count*12);

            using (var outStream = new BinaryHeapStream(estimatedCapacity))
            {
                BinaryWriter writer = _binary.Marshaller.StartMarshal(outStream);

                writer.SetBuilder(this);

                // All related builders will work in this context with this writer.
                _parent._ctx = new Context(writer);
            
                try
                {
                    // Write.
                    writer.Write(this);
                
                    // Process metadata.
                    _binary.Marshaller.FinishMarshal(writer);

                    // Create binary object once metadata is processed.
                    return new BinaryObject(_binary.Marshaller, outStream.InternalArray, 0, 
                        BinaryObjectHeader.Read(outStream, 0));
                }
                finally
                {
                    // Cleanup.
                    _parent._ctx.Closed = true;
                }
            }
        }

        /// <summary>
        /// Create child builder.
        /// </summary>
        /// <param name="obj">binary object.</param>
        /// <returns>Child builder.</returns>
        public BinaryObjectBuilder Child(BinaryObject obj)
        {
            var desc = _binary.Marshaller.GetDescriptor(true, obj.TypeId);

            return new BinaryObjectBuilder(_binary, null, obj, desc);
        }
        
        /// <summary>
        /// Get cache field.
        /// </summary>
        /// <param name="pos">Position.</param>
        /// <param name="val">Value.</param>
        /// <returns><c>true</c> if value is found in cache.</returns>
        public bool TryGetCachedField<T>(int pos, out T val)
        {
            if (_parent._cache != null)
            {
                BinaryBuilderField res;

                if (_parent._cache.TryGetValue(pos, out res))
                {
                    val = res != null ? (T) res.Value : default(T);

                    return true;
                }
            }

            val = default(T);

            return false;
        }

        /// <summary>
        /// Add field to cache test.
        /// </summary>
        /// <param name="pos">Position.</param>
        /// <param name="val">Value.</param>
        public BinaryBuilderField CacheField<T>(int pos, T val)
        {
            if (_parent._cache == null)
                _parent._cache = new Dictionary<int, BinaryBuilderField>(2);

            var hdr = _obj.Data[pos];

            var field = new BinaryBuilderField(typeof(T), val, hdr, GetWriteAction(hdr, pos));
            
            _parent._cache[pos] = field;

            return field;
        }

        /// <summary>
        /// Gets the write action by header.
        /// </summary>
        /// <param name="header">The header.</param>
        /// <param name="pos">Position.</param>
        /// <returns>Write action.</returns>
        private Action<BinaryWriter, object> GetWriteAction(byte header, int pos)
        {
            // We need special actions for all cases where SetField(X) produces different result from SetSpecialField(X)
            // Arrays, Collections, Dates

            switch (header)
            {
                case BinaryUtils.TypeArray:
                    return WriteArrayAction;

                case BinaryUtils.TypeCollection:
                    return WriteCollectionAction;

                case BinaryUtils.TypeTimestamp:
                    return WriteTimestampAction;

                case BinaryUtils.TypeArrayTimestamp:
                    return WriteTimestampArrayAction;

                case BinaryUtils.TypeArrayEnum:
                    using (var stream = new BinaryHeapStream(_obj.Data))
                    {
                        stream.Seek(pos, SeekOrigin.Begin + 1);

                        var elementTypeId = stream.ReadInt();

                        return (w, o) => w.WriteEnumArrayInternal((Array) o, elementTypeId);
                    }

                default:
                    return null;
            }
        }

        /// <summary>
        /// Internal set field routine.
        /// </summary>
        /// <param name="fieldName">Name.</param>
        /// <param name="val">Value.</param>
        /// <returns>This builder.</returns>
        private IBinaryObjectBuilder SetField0(string fieldName, BinaryBuilderField val)
        {
            if (_vals == null)
                _vals = new Dictionary<string, BinaryBuilderField>();

            _vals[fieldName] = val;

            return this;
        }

        /// <summary>
        /// Mutate binary object.
        /// </summary>
        /// <param name="inStream">Input stream with initial object.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="desc">Type descriptor.</param>
        /// <param name="hashCode">Hash code.</param>
        /// <param name="vals">Values.</param>
        private void Mutate(
            BinaryHeapStream inStream,
            BinaryHeapStream outStream,
            IBinaryTypeDescriptor desc,
            int? hashCode, 
            IDictionary<string, BinaryBuilderField> vals)
        {
            // Set correct builder to writer frame.
            BinaryObjectBuilder oldBuilder = _parent._ctx.Writer.SetBuilder(_parent);

            int streamPos = inStream.Position;
            
            try
            {
                // Prepare fields.
                IBinaryTypeHandler metaHnd = _binary.Marshaller.GetBinaryTypeHandler(desc);

                IDictionary<int, BinaryBuilderField> vals0;

                if (vals == null || vals.Count == 0)
                    vals0 = EmptyVals;
                else
                {
                    vals0 = new Dictionary<int, BinaryBuilderField>(vals.Count);

                    foreach (KeyValuePair<string, BinaryBuilderField> valEntry in vals)
                    {
                        int fieldId = BinaryUtils.FieldId(desc.TypeId, valEntry.Key, desc.NameMapper, desc.IdMapper);

                        if (vals0.ContainsKey(fieldId))
                            throw new IgniteException("Collision in field ID detected (change field name or " +
                                "define custom ID mapper) [fieldName=" + valEntry.Key + ", fieldId=" + fieldId + ']');

                        vals0[fieldId] = valEntry.Value;

                        // Write metadata if: 1) it is enabled for type; 2) type is not null (i.e. it is neither 
                        // remove marker, nor a field read through "GetField" method.
                        if (metaHnd != null && valEntry.Value.Type != null)
                            metaHnd.OnFieldWrite(fieldId, valEntry.Key, valEntry.Value.TypeId);
                    }
                }

                // Actual processing.
                Mutate0(_parent._ctx, inStream, outStream, true, hashCode, vals0);

                // 3. Handle metadata.
                if (metaHnd != null)
                {
                    IDictionary<string, int> meta = metaHnd.OnObjectWriteFinished();

                    if (meta != null)
                        _parent._ctx.Writer.SaveMetadata(desc, meta);
                }
            }
            finally
            {
                // Restore builder frame.
                _parent._ctx.Writer.SetBuilder(oldBuilder);

                inStream.Seek(streamPos, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Internal mutation routine.
        /// </summary>
        /// <param name="inStream">Input stream.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="ctx">Context.</param>
        /// <param name="changeHash">WHether hash should be changed.</param>
        /// <param name="hash">New hash.</param>
        /// <param name="vals">Values to be replaced.</param>
        /// <returns>Mutated object.</returns>
        private void Mutate0(Context ctx, BinaryHeapStream inStream, IBinaryStream outStream,
            bool changeHash, int? hash, IDictionary<int, BinaryBuilderField> vals)
        {
            int inStartPos = inStream.Position;
            int outStartPos = outStream.Position;

            byte inHdr = inStream.ReadByte();

            if (inHdr == BinaryUtils.HdrNull)
                outStream.WriteByte(BinaryUtils.HdrNull);
            else if (inHdr == BinaryUtils.HdrHnd)
            {
                int inHnd = inStream.ReadInt();

                int oldPos = inStartPos - inHnd;
                int newPos;

                if (ctx.OldToNew(oldPos, out newPos))
                {
                    // Handle is still valid.
                    outStream.WriteByte(BinaryUtils.HdrHnd);
                    outStream.WriteInt(outStartPos - newPos);
                }
                else
                {
                    // Handle is invalid, write full object.
                    int inRetPos = inStream.Position;

                    inStream.Seek(oldPos, SeekOrigin.Begin);

                    Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);

                    inStream.Seek(inRetPos, SeekOrigin.Begin);
                }
            }
            else if (inHdr == BinaryUtils.HdrFull)
            {
                var inHeader = BinaryObjectHeader.Read(inStream, inStartPos);
                
                BinaryUtils.ValidateProtocolVersion(inHeader.Version);

                int hndPos;

                if (ctx.AddOldToNew(inStartPos, outStartPos, out hndPos))
                {
                    // Object could be cached in parent builder.
                    BinaryBuilderField cachedVal;

                    if (_parent._cache != null && _parent._cache.TryGetValue(inStartPos, out cachedVal))
                    {
                        WriteField(ctx, cachedVal);
                    }
                    else
                    {
                        // New object, write in full form.
                        var inSchema = BinaryObjectSchemaSerializer.ReadSchema(inStream, inStartPos, inHeader, 
                            _desc.Schema, _binary.Marshaller);

                        var outSchema = BinaryObjectSchemaHolder.Current;
                        var schemaIdx = outSchema.PushSchema();

                        try
                        {
                            // Skip header as it is not known at this point.
                            outStream.Seek(BinaryObjectHeader.Size, SeekOrigin.Current);

                            if (inSchema != null)
                            {
                                foreach (var inField in inSchema)
                                {
                                    BinaryBuilderField fieldVal;

                                    var fieldFound = vals.TryGetValue(inField.Id, out fieldVal);

                                    if (fieldFound && fieldVal == BinaryBuilderField.RmvMarker)
                                        continue;

                                    outSchema.PushField(inField.Id, outStream.Position - outStartPos);

                                    if (!fieldFound)
                                        fieldFound = _parent._cache != null &&
                                                     _parent._cache.TryGetValue(inField.Offset + inStartPos,
                                                         out fieldVal);

                                    if (fieldFound)
                                    {
                                        WriteField(ctx, fieldVal);

                                        vals.Remove(inField.Id);
                                    }
                                    else
                                    {
                                        // Field is not tracked, re-write as is.
                                        inStream.Seek(inField.Offset + inStartPos, SeekOrigin.Begin);

                                        Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);
                                    }
                                }
                            }

                            // Write remaining new fields.
                            foreach (var valEntry in vals)
                            {
                                if (valEntry.Value == BinaryBuilderField.RmvMarker)
                                    continue;

                                outSchema.PushField(valEntry.Key, outStream.Position - outStartPos);

                                WriteField(ctx, valEntry.Value);
                            }

                            var flags = inHeader.IsUserType
                                ? BinaryObjectHeader.Flag.UserType
                                : BinaryObjectHeader.Flag.None;

                            // Write raw data.
                            int outRawOff = outStream.Position - outStartPos;

                            if (inHeader.HasRaw)
                            {
                                var inRawOff = inHeader.GetRawOffset(inStream, inStartPos);
                                var inRawLen = inHeader.SchemaOffset - inRawOff;

                                flags |= BinaryObjectHeader.Flag.HasRaw;

                                outStream.Write(inStream.InternalArray, inStartPos + inRawOff, inRawLen);
                            }

                            // Write schema
                            int outSchemaOff = outRawOff;
                            var schemaPos = outStream.Position;
                            int outSchemaId;

                            if (inHeader.IsCompactFooter)
                                flags |= BinaryObjectHeader.Flag.CompactFooter;

                            var hasSchema = outSchema.WriteSchema(outStream, schemaIdx, out outSchemaId, ref flags);

                            if (hasSchema)
                            {
                                outSchemaOff = schemaPos - outStartPos;
                                
                                flags |= BinaryObjectHeader.Flag.HasSchema;

                                if (inHeader.HasRaw)
                                    outStream.WriteInt(outRawOff);

                                if (_desc.Schema.Get(outSchemaId) == null)
                                    _desc.Schema.Add(outSchemaId, outSchema.GetSchema(schemaIdx));
                            }

                            var outLen = outStream.Position - outStartPos;

                            var outHash = inHeader.HashCode;

                            if (changeHash)
                            {
                                if (hash != null)
                                {
                                    outHash = hash.Value;
                                }
                                else
                                {
                                    // Get from identity resolver.
                                    outHash = _desc.EqualityComparer != null
                                        ? _desc.EqualityComparer.GetHashCode(outStream,
                                            outStartPos + BinaryObjectHeader.Size,
                                            schemaPos - outStartPos - BinaryObjectHeader.Size,
                                            outSchema, outSchemaId, _binary.Marshaller, _desc)
                                        : 0;
                                }
                            }

                            var outHeader = new BinaryObjectHeader(inHeader.TypeId, outHash, outLen, 
                                outSchemaId, outSchemaOff, flags);

                            BinaryObjectHeader.Write(outHeader, outStream, outStartPos);

                            outStream.Seek(outStartPos + outLen, SeekOrigin.Begin);  // seek to the end of the object
                        }
                        finally
                        {
                            outSchema.PopSchema(schemaIdx);
                        }
                    }
                }
                else
                {
                    // Object has already been written, write as handle.
                    outStream.WriteByte(BinaryUtils.HdrHnd);
                    outStream.WriteInt(outStartPos - hndPos);
                }

                // Synchronize input stream position.
                inStream.Seek(inStartPos + inHeader.Length, SeekOrigin.Begin);
            }
            else
            {
                // Try writing as well-known type with fixed size.
                outStream.WriteByte(inHdr);

                if (!WriteAsPredefined(inHdr, inStream, outStream, ctx))
                    throw new IgniteException("Unexpected header [position=" + (inStream.Position - 1) +
                        ", header=" + inHdr + ']');
            }
        }

        /// <summary>
        /// Writes the specified field.
        /// </summary>
        private static void WriteField(Context ctx, BinaryBuilderField field)
        {
            var action = field.WriteAction;

            if (action != null)
                action(ctx.Writer, field.Value);
            else
                ctx.Writer.Write(field.Value);
        }

        /// <summary>
        /// Process binary object inverting handles if needed.
        /// </summary>
        /// <param name="outStream">Output stream.</param>
        /// <param name="port">Binary object.</param>
        internal void ProcessBinary(IBinaryStream outStream, BinaryObject port)
        {
            // Special case: writing binary object with correct inversions.
            using (var inStream = new BinaryHeapStream(port.Data))
            {
                inStream.Seek(port.Offset, SeekOrigin.Begin);

                // Use fresh context to ensure correct binary inversion.
                Mutate0(new Context(), inStream, outStream, false, 0, EmptyVals);
            }
        }

        /// <summary>
        /// Process child builder.
        /// </summary>
        /// <param name="outStream">Output stream.</param>
        /// <param name="builder">Builder.</param>
        internal void ProcessBuilder(IBinaryStream outStream, BinaryObjectBuilder builder)
        {
            using (var inStream = new BinaryHeapStream(builder._obj.Data))
            {
                inStream.Seek(builder._obj.Offset, SeekOrigin.Begin);

                // Builder parent context might be null only in one case: if we never met this group of
                // builders before. In this case we set context to their parent and track it. Context
                // cleanup will be performed at the very end of build process.
                if (builder._parent._ctx == null || builder._parent._ctx.Closed)
                    builder._parent._ctx = new Context(_parent._ctx);

                builder.Mutate(inStream, (BinaryHeapStream) outStream, builder._desc,
                    builder._hashCode, builder._vals);
            }
        }

        /// <summary>
        /// Write object as a predefined type if possible.
        /// </summary>
        /// <param name="hdr">Header.</param>
        /// <param name="inStream">Input stream.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="ctx">Context.</param>
        /// <returns><c>True</c> if was written.</returns>
        private bool WriteAsPredefined(byte hdr, BinaryHeapStream inStream, IBinaryStream outStream,
            Context ctx)
        {
            switch (hdr)
            {
                case BinaryUtils.TypeByte:
                    TransferBytes(inStream, outStream, 1);

                    break;

                case BinaryUtils.TypeShort:
                    TransferBytes(inStream, outStream, 2);

                    break;

                case BinaryUtils.TypeInt:
                    TransferBytes(inStream, outStream, 4);

                    break;

                case BinaryUtils.TypeLong:
                    TransferBytes(inStream, outStream, 8);

                    break;

                case BinaryUtils.TypeFloat:
                    TransferBytes(inStream, outStream, 4);

                    break;

                case BinaryUtils.TypeDouble:
                    TransferBytes(inStream, outStream, 8);

                    break;

                case BinaryUtils.TypeChar:
                    TransferBytes(inStream, outStream, 2);

                    break;

                case BinaryUtils.TypeBool:
                    TransferBytes(inStream, outStream, 1);

                    break;

                case BinaryUtils.TypeDecimal:
                    TransferBytes(inStream, outStream, 4); // Transfer scale

                    int magLen = inStream.ReadInt(); // Transfer magnitude length.

                    outStream.WriteInt(magLen);

                    TransferBytes(inStream, outStream, magLen); // Transfer magnitude.

                    break;

                case BinaryUtils.TypeString:
                    BinaryUtils.WriteString(BinaryUtils.ReadString(inStream), outStream);

                    break;

                case BinaryUtils.TypeGuid:
                    TransferBytes(inStream, outStream, 16);

                    break;

                case BinaryUtils.TypeTimestamp:
                    TransferBytes(inStream, outStream, 12);

                    break;

                case BinaryUtils.TypeArrayByte:
                    TransferArray(inStream, outStream, 1);

                    break;

                case BinaryUtils.TypeArrayShort:
                    TransferArray(inStream, outStream, 2);

                    break;

                case BinaryUtils.TypeArrayInt:
                    TransferArray(inStream, outStream, 4);

                    break;

                case BinaryUtils.TypeArrayLong:
                    TransferArray(inStream, outStream, 8);

                    break;

                case BinaryUtils.TypeArrayFloat:
                    TransferArray(inStream, outStream, 4);

                    break;

                case BinaryUtils.TypeArrayDouble:
                    TransferArray(inStream, outStream, 8);

                    break;

                case BinaryUtils.TypeArrayChar:
                    TransferArray(inStream, outStream, 2);

                    break;

                case BinaryUtils.TypeArrayBool:
                    TransferArray(inStream, outStream, 1);

                    break;

                case BinaryUtils.TypeArrayDecimal:
                case BinaryUtils.TypeArrayString:
                case BinaryUtils.TypeArrayGuid:
                case BinaryUtils.TypeArrayTimestamp:
                case BinaryUtils.TypeArrayEnum:
                case BinaryUtils.TypeArray:
                    int arrLen = inStream.ReadInt();

                    outStream.WriteInt(arrLen);

                    for (int i = 0; i < arrLen; i++)
                        Mutate0(ctx, inStream, outStream, false, 0, null);

                    break;

                case BinaryUtils.TypeCollection:
                    int colLen = inStream.ReadInt();

                    outStream.WriteInt(colLen);

                    outStream.WriteByte(inStream.ReadByte());

                    for (int i = 0; i < colLen; i++)
                        Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);

                    break;

                case BinaryUtils.TypeDictionary:
                    int dictLen = inStream.ReadInt();

                    outStream.WriteInt(dictLen);

                    outStream.WriteByte(inStream.ReadByte());

                    for (int i = 0; i < dictLen; i++)
                    {
                        Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);
                        Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);
                    }

                    break;
                    
                case BinaryUtils.TypeBinary:
                    TransferArray(inStream, outStream, 1); // Data array.
                    TransferBytes(inStream, outStream, 4); // Offset in array.

                    break;

                case BinaryUtils.TypeEnum:
                    TransferBytes(inStream, outStream, 4); // Integer ordinal.

                    break;

                default:
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Transfer bytes from one stream to another.
        /// </summary>
        /// <param name="inStream">Input stream.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="cnt">Bytes count.</param>
        private static void TransferBytes(BinaryHeapStream inStream, IBinaryStream outStream, int cnt)
        {
            outStream.Write(inStream.InternalArray, inStream.Position, cnt);

            inStream.Seek(cnt, SeekOrigin.Current);
        }

        /// <summary>
        /// Transfer array of fixed-size elements from one stream to another.
        /// </summary>
        /// <param name="inStream">Input stream.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="elemSize">Element size.</param>
        private static void TransferArray(BinaryHeapStream inStream, IBinaryStream outStream,
            int elemSize)
        {
            int len = inStream.ReadInt();

            outStream.WriteInt(len);

            TransferBytes(inStream, outStream, elemSize * len);
        }

        /// <summary>
        /// Create empty binary object from descriptor.
        /// </summary>
        /// <param name="desc">Descriptor.</param>
        /// <returns>Empty binary object.</returns>
        private BinaryObject BinaryFromDescriptor(IBinaryTypeDescriptor desc)
        {
            const int len = BinaryObjectHeader.Size;

            var flags = desc.UserType ? BinaryObjectHeader.Flag.UserType : BinaryObjectHeader.Flag.None;

            if (_binary.Marshaller.CompactFooter && desc.UserType)
                flags |= BinaryObjectHeader.Flag.CompactFooter;

            var hdr = new BinaryObjectHeader(desc.TypeId, 0, len, 0, len, flags);

            using (var stream = new BinaryHeapStream(len))
            {
                BinaryObjectHeader.Write(hdr, stream, 0);

                return new BinaryObject(_binary.Marshaller, stream.InternalArray, 0, hdr);
            }
        }

        /// <summary>
        /// Mutation context.
        /// </summary>
        private class Context
        {
            /** Map from object position in old binary to position in new binary. */
            private IDictionary<int, int> _oldToNew;

            /** Parent context. */
            private readonly Context _parent;

            /** Binary writer. */
            private readonly BinaryWriter _writer;

            /** Children contexts. */
            private ICollection<Context> _children;

            /** Closed flag; if context is closed, it can no longer be used. */
            private bool _closed;

            /// <summary>
            /// Constructor for parent context where writer invocation is not expected.
            /// </summary>
            public Context()
            {
                // No-op.
            }

            /// <summary>
            /// Constructor for parent context.
            /// </summary>
            /// <param name="writer">Writer</param>
            public Context(BinaryWriter writer)
            {
                _writer = writer;
            }

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="parent">Parent context.</param>
            public Context(Context parent)
            {
                _parent = parent;
                
                _writer = parent._writer;

                if (parent._children == null)
                    parent._children = new List<Context>();

                parent._children.Add(this);
            }

            /// <summary>
            /// Add another old-to-new position mapping.
            /// </summary>
            /// <param name="oldPos">Old position.</param>
            /// <param name="newPos">New position.</param>
            /// <param name="hndPos">Handle position.</param>
            /// <returns><c>True</c> if ampping was added, <c>false</c> if mapping already existed and handle
            /// position in the new object is returned.</returns>
            public bool AddOldToNew(int oldPos, int newPos, out int hndPos)
            {
                if (_oldToNew == null)
                    _oldToNew = new Dictionary<int, int>();

                if (_oldToNew.TryGetValue(oldPos, out hndPos))
                    return false;
                _oldToNew[oldPos] = newPos;

                return true;
            }

            /// <summary>
            /// Get mapping of old position to the new one.
            /// </summary>
            /// <param name="oldPos">Old position.</param>
            /// <param name="newPos">New position.</param>
            /// <returns><c>True</c> if mapping exists.</returns>
            public bool OldToNew(int oldPos, out int newPos)
            {
                return _oldToNew.TryGetValue(oldPos, out newPos);
            }

            /// <summary>
            /// Writer.
            /// </summary>
            public BinaryWriter Writer
            {
                get { return _writer; }
            }

            /// <summary>
            /// Closed flag.
            /// </summary>
            public bool Closed
            {
                get
                {
                    return _closed;
                }
                set
                {
                    Context ctx = this;

                    while (ctx != null)
                    {
                        ctx._closed = value;

                        if (_children != null) {
                            foreach (Context child in _children)
                                child.Closed = value;
                        }

                        ctx = ctx._parent;
                    }
                }
            }
        }
    }
}
