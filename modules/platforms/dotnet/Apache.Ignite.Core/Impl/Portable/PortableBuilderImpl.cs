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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Portable.Metadata;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable builder implementation.
    /// </summary>
    internal class PortableBuilderImpl : IPortableBuilder
    {
        /** Cached dictionary with no values. */
        private static readonly IDictionary<int, PortableBuilderField> EmptyVals =
            new Dictionary<int, PortableBuilderField>();
        
        /** Portables. */
        private readonly PortablesImpl _portables;

        /** */
        private readonly PortableBuilderImpl _parent;

        /** Initial portable object. */
        private readonly PortableUserObject _obj;

        /** Type descriptor. */
        private readonly IPortableTypeDescriptor _desc;

        /** Values. */
        private IDictionary<string, PortableBuilderField> _vals;

        /** Contextual fields. */
        private IDictionary<int, PortableBuilderField> _cache;

        /** Hash code. */
        private int _hashCode;
        
        /** Current context. */
        private Context _ctx;

        /** Write array action. */
        private static readonly Action<PortableWriterImpl, object> WriteArrayAction = 
            (w, o) => w.WriteArrayInternal((Array) o);

        /** Write collection action. */
        private static readonly Action<PortableWriterImpl, object> WriteCollectionAction = 
            (w, o) => w.WriteCollection((ICollection) o);

        /** Write timestamp action. */
        private static readonly Action<PortableWriterImpl, object> WriteTimestampAction = 
            (w, o) => w.WriteTimestamp((DateTime?) o);

        /** Write timestamp array action. */
        private static readonly Action<PortableWriterImpl, object> WriteTimestampArrayAction = 
            (w, o) => w.WriteTimestampArray((DateTime?[])o);

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="portables">Portables.</param>
        /// <param name="parent">Parent builder.</param>
        /// <param name="obj">Initial portable object.</param>
        /// <param name="desc">Type descriptor.</param>
        public PortableBuilderImpl(PortablesImpl portables, PortableBuilderImpl parent, 
            PortableUserObject obj, IPortableTypeDescriptor desc)
        {
            Debug.Assert(portables != null);
            Debug.Assert(obj != null);
            Debug.Assert(desc != null);

            _portables = portables;
            _parent = parent ?? this;
            _obj = obj;
            _desc = desc;

            _hashCode = obj.GetHashCode();
        }

        /** <inheritDoc /> */
        public IPortableBuilder SetHashCode(int hashCode)
        {
            _hashCode = hashCode;

            return this;
        }

        /** <inheritDoc /> */
        public T GetField<T>(string name)
        {
            PortableBuilderField field;

            if (_vals != null && _vals.TryGetValue(name, out field))
                return field != PortableBuilderField.RmvMarker ? (T) field.Value : default(T);

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
        public IPortableBuilder SetField<T>(string fieldName, T val)
        {
            return SetField0(fieldName,
                new PortableBuilderField(typeof (T), val, PortableSystemHandlers.GetTypeId(typeof (T))));
        }

        /** <inheritDoc /> */
        public IPortableBuilder SetArrayField<T>(string fieldName, T[] val)
        {
            return SetField0(fieldName,
                new PortableBuilderField(typeof (T[]), val, PortableUtils.TypeArray, WriteArrayAction));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetBooleanField(string fieldName, bool val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (bool), val, PortableUtils.TypeBool, 
                (w, o) => w.WriteBoolean((bool) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetBooleanArrayField(string fieldName, bool[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (bool[]), val, PortableUtils.TypeArrayBool,
                (w, o) => w.WriteBooleanArray((bool[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetByteField(string fieldName, byte val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (byte), val, PortableUtils.TypeByte,
                (w, o) => w.WriteByte((byte) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetByteArrayField(string fieldName, byte[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (byte[]), val, PortableUtils.TypeArrayByte,
                (w, o) => w.WriteByteArray((byte[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetCharField(string fieldName, char val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (char), val, PortableUtils.TypeChar,
                (w, o) => w.WriteChar((char) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetCharArrayField(string fieldName, char[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (char[]), val, PortableUtils.TypeArrayChar,
                (w, o) => w.WriteCharArray((char[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetCollectionField(string fieldName, ICollection val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (ICollection), val, PortableUtils.TypeCollection,
                WriteCollectionAction));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetDecimalField(string fieldName, decimal? val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (decimal?), val, PortableUtils.TypeDecimal,
                (w, o) => w.WriteDecimal((decimal?) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetDecimalArrayField(string fieldName, decimal?[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (decimal?[]), val, PortableUtils.TypeArrayDecimal,
                (w, o) => w.WriteDecimalArray((decimal?[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetDictionaryField(string fieldName, IDictionary val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (IDictionary), val, PortableUtils.TypeDictionary,
                (w, o) => w.WriteDictionary((IDictionary) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetDoubleField(string fieldName, double val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (double), val, PortableUtils.TypeDouble,
                (w, o) => w.WriteDouble((double) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetDoubleArrayField(string fieldName, double[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (double[]), val, PortableUtils.TypeArrayDouble,
                (w, o) => w.WriteDoubleArray((double[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetEnumField<T>(string fieldName, T val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (T), val, PortableUtils.TypeEnum,
                (w, o) => w.WriteEnum((T) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetEnumArrayField<T>(string fieldName, T[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (T[]), val, PortableUtils.TypeArrayEnum,
                (w, o) => w.WriteEnumArray((T[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetFloatField(string fieldName, float val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (float), val, PortableUtils.TypeFloat,
                (w, o) => w.WriteFloat((float) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetFloatArrayField(string fieldName, float[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (float[]), val, PortableUtils.TypeArrayFloat,
                (w, o) => w.WriteFloatArray((float[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetGuidField(string fieldName, Guid? val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (Guid?), val, PortableUtils.TypeGuid,
                (w, o) => w.WriteGuid((Guid?) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetGuidArrayField(string fieldName, Guid?[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (Guid?[]), val, PortableUtils.TypeArrayGuid,
                (w, o) => w.WriteGuidArray((Guid?[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetIntField(string fieldName, int val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (int), val, PortableUtils.TypeInt,
                (w, o) => w.WriteInt((int) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetIntArrayField(string fieldName, int[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (int[]), val, PortableUtils.TypeArrayInt,
                (w, o) => w.WriteIntArray((int[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetLongField(string fieldName, long val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (long), val, PortableUtils.TypeLong,
                (w, o) => w.WriteLong((long) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetLongArrayField(string fieldName, long[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (long[]), val, PortableUtils.TypeArrayLong,
                (w, o) => w.WriteLongArray((long[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetShortField(string fieldName, short val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (short), val, PortableUtils.TypeShort,
                (w, o) => w.WriteShort((short) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetShortArrayField(string fieldName, short[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (short[]), val, PortableUtils.TypeArrayShort,
                (w, o) => w.WriteShortArray((short[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetStringField(string fieldName, string val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (string), val, PortableUtils.TypeString,
                (w, o) => w.WriteString((string) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetStringArrayField(string fieldName, string[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (string[]), val, PortableUtils.TypeArrayString,
                (w, o) => w.WriteStringArray((string[]) o)));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetTimestampField(string fieldName, DateTime? val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (DateTime?), val, PortableUtils.TypeTimestamp,
                WriteTimestampAction));
        }
 
        /** <inheritDoc /> */
        public IPortableBuilder SetTimestampArrayField(string fieldName, DateTime?[] val)
        {
            return SetField0(fieldName, new PortableBuilderField(typeof (DateTime?[]), val, PortableUtils.TypeArrayTimestamp,
                WriteTimestampArrayAction));
        } 

        /** <inheritDoc /> */
        public IPortableBuilder RemoveField(string name)
        {
            return SetField0(name, PortableBuilderField.RmvMarker);
        }

        /** <inheritDoc /> */
        public IPortableObject Build()
        {
            PortableHeapStream inStream = new PortableHeapStream(_obj.Data);

            inStream.Seek(_obj.Offset, SeekOrigin.Begin);

            // Assume that resulting length will be no less than header + [fields_cnt] * 12;
            int len = PortableUtils.FullHdrLen + (_vals == null ? 0 : _vals.Count * 12);

            PortableHeapStream outStream = new PortableHeapStream(len);

            PortableWriterImpl writer = _portables.Marshaller.StartMarshal(outStream);

            writer.SetBuilder(this);

            // All related builders will work in this context with this writer.
            _parent._ctx = new Context(writer);
            
            try
            {
                // Write.
                writer.Write(this);
                
                // Process metadata.
                _portables.Marshaller.FinishMarshal(writer);

                // Create portable object once metadata is processed.
                return new PortableUserObject(_portables.Marshaller, outStream.InternalArray, 0,
                    _desc.TypeId, _hashCode);
            }
            finally
            {
                // Cleanup.
                _parent._ctx.Closed = true;
            }
        }

        /// <summary>
        /// Create child builder.
        /// </summary>
        /// <param name="obj">Portable object.</param>
        /// <returns>Child builder.</returns>
        public PortableBuilderImpl Child(PortableUserObject obj)
        {
            var desc = _portables.Marshaller.GetDescriptor(true, obj.TypeId);

            return new PortableBuilderImpl(_portables, null, obj, desc);
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
                PortableBuilderField res;

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
        public PortableBuilderField CacheField<T>(int pos, T val)
        {
            if (_parent._cache == null)
                _parent._cache = new Dictionary<int, PortableBuilderField>(2);

            var hdr = _obj.Data[pos];

            var field = new PortableBuilderField(typeof(T), val, hdr, GetWriteAction(hdr));
            
            _parent._cache[pos] = field;

            return field;
        }

        /// <summary>
        /// Gets the write action by header.
        /// </summary>
        /// <param name="header">The header.</param>
        /// <returns>Write action.</returns>
        private static Action<PortableWriterImpl, object> GetWriteAction(byte header)
        {
            // We need special actions for all cases where SetField(X) produces different result from SetSpecialField(X)
            // Arrays, Collections, Dates

            switch (header)
            {
                case PortableUtils.TypeArray:
                    return WriteArrayAction;

                case PortableUtils.TypeCollection:
                    return WriteCollectionAction;

                case PortableUtils.TypeTimestamp:
                    return WriteTimestampAction;

                case PortableUtils.TypeArrayTimestamp:
                    return WriteTimestampArrayAction;
            }

            return null;
        }

        /// <summary>
        /// Internal set field routine.
        /// </summary>
        /// <param name="fieldName">Name.</param>
        /// <param name="val">Value.</param>
        /// <returns>This builder.</returns>
        private IPortableBuilder SetField0(string fieldName, PortableBuilderField val)
        {
            if (_vals == null)
                _vals = new Dictionary<string, PortableBuilderField>();

            _vals[fieldName] = val;

            return this;
        }

        /// <summary>
        /// Mutate portable object.
        /// </summary>
        /// <param name="inStream">Input stream with initial object.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="desc">Portable type descriptor.</param>
        /// <param name="hashCode">Hash code.</param>
        /// <param name="vals">Values.</param>
        private void Mutate(
            PortableHeapStream inStream,
            PortableHeapStream outStream,
            IPortableTypeDescriptor desc,
            int hashCode, 
            IDictionary<string, PortableBuilderField> vals)
        {
            // Set correct builder to writer frame.
            PortableBuilderImpl oldBuilder = _parent._ctx.Writer.SetBuilder(_parent);

            int streamPos = inStream.Position;
            
            try
            {
                // Prepare fields.
                IPortableMetadataHandler metaHnd = _portables.Marshaller.GetMetadataHandler(desc);

                IDictionary<int, PortableBuilderField> vals0;

                if (vals == null || vals.Count == 0)
                    vals0 = EmptyVals;
                else
                {
                    vals0 = new Dictionary<int, PortableBuilderField>(vals.Count);

                    foreach (KeyValuePair<string, PortableBuilderField> valEntry in vals)
                    {
                        int fieldId = PortableUtils.FieldId(desc.TypeId, valEntry.Key, desc.NameConverter, desc.Mapper);

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
                        _parent._ctx.Writer.SaveMetadata(desc.TypeId, desc.TypeName, desc.AffinityKeyFieldName, meta);
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
        private void Mutate0(Context ctx, PortableHeapStream inStream, IPortableStream outStream,
            bool changeHash, int hash, IDictionary<int, PortableBuilderField> vals)
        {
            int inStartPos = inStream.Position;
            int outStartPos = outStream.Position;

            byte inHdr = inStream.ReadByte();

            if (inHdr == PortableUtils.HdrNull)
                outStream.WriteByte(PortableUtils.HdrNull);
            else if (inHdr == PortableUtils.HdrHnd)
            {
                int inHnd = inStream.ReadInt();

                int oldPos = inStartPos - inHnd;
                int newPos;

                if (ctx.OldToNew(oldPos, out newPos))
                {
                    // Handle is still valid.
                    outStream.WriteByte(PortableUtils.HdrHnd);
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
            else if (inHdr == PortableUtils.HdrFull)
            {
                // TODO: PortableObjectHeader
                PortableUtils.ValidateProtocolVersion(inStream.ReadByte());

                byte inUsrFlag = inStream.ReadByte();
                int inTypeId = inStream.ReadInt();
                int inHash = inStream.ReadInt();
                int inLen = inStream.ReadInt();
                int inRawOff = inStream.ReadInt();

                int hndPos;

                if (ctx.AddOldToNew(inStartPos, outStartPos, out hndPos))
                {
                    // Object could be cached in parent builder.
                    PortableBuilderField cachedVal;

                    if (_parent._cache != null && _parent._cache.TryGetValue(inStartPos, out cachedVal)) {
                        WriteField(ctx, cachedVal);
                    }
                    else
                    {
                        // New object, write in full form.
                        outStream.WriteByte(PortableUtils.HdrFull);
                        outStream.WriteByte(PortableUtils.ProtoVer);
                        outStream.WriteByte(inUsrFlag);
                        outStream.WriteInt(inTypeId);
                        outStream.WriteInt(changeHash ? hash : inHash);

                        // Skip length and raw offset as they are not known at this point.
                        outStream.Seek(8, SeekOrigin.Current);

                        // Write regular fields.
                        while (inStream.Position < inStartPos + inRawOff)
                        {
                            int inFieldId = inStream.ReadInt();
                            int inFieldLen = inStream.ReadInt();
                            int inFieldDataPos = inStream.Position;

                            PortableBuilderField fieldVal;

                            bool fieldFound = vals.TryGetValue(inFieldId, out fieldVal);

                            if (!fieldFound || fieldVal != PortableBuilderField.RmvMarker)
                            {
                                outStream.WriteInt(inFieldId);

                                int fieldLenPos = outStream.Position; // Here we will write length later.

                                outStream.Seek(4, SeekOrigin.Current);

                                if (fieldFound)
                                {
                                    // Replace field with new value.
                                    if (fieldVal != PortableBuilderField.RmvMarker)
                                        WriteField(ctx, fieldVal);

                                    vals.Remove(inFieldId);
                                }
                                else
                                {
                                    // If field was requested earlier, then we must write tracked value
                                    if (_parent._cache != null && _parent._cache.TryGetValue(inFieldDataPos, out fieldVal))
                                        WriteField(ctx, fieldVal);
                                    else
                                        // Field is not tracked, re-write as is.
                                        Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);                                    
                                }

                                int fieldEndPos = outStream.Position;

                                outStream.Seek(fieldLenPos, SeekOrigin.Begin);
                                outStream.WriteInt(fieldEndPos - fieldLenPos - 4);
                                outStream.Seek(fieldEndPos, SeekOrigin.Begin);
                            }

                            // Position intput stream pointer after the field.
                            inStream.Seek(inFieldDataPos + inFieldLen, SeekOrigin.Begin);
                        }

                        // Write remaining new fields.
                        foreach (var valEntry in vals)
                        {
                            if (valEntry.Value != PortableBuilderField.RmvMarker)
                            {
                                outStream.WriteInt(valEntry.Key);

                                int fieldLenPos = outStream.Position; // Here we will write length later.

                                outStream.Seek(4, SeekOrigin.Current);

                                WriteField(ctx, valEntry.Value);

                                int fieldEndPos = outStream.Position;

                                outStream.Seek(fieldLenPos, SeekOrigin.Begin);
                                outStream.WriteInt(fieldEndPos - fieldLenPos - 4);
                                outStream.Seek(fieldEndPos, SeekOrigin.Begin);
                            }
                        }

                        // Write raw data.
                        int rawPos = outStream.Position;

                        outStream.Write(inStream.InternalArray, inStartPos + inRawOff, inLen - inRawOff);

                        // Write length and raw data offset.
                        int outResPos = outStream.Position;

                        outStream.Seek(outStartPos + PortableUtils.OffsetLen, SeekOrigin.Begin);

                        outStream.WriteInt(outResPos - outStartPos); // Length.
                        outStream.WriteInt(rawPos - outStartPos); // Raw offset.

                        outStream.Seek(outResPos, SeekOrigin.Begin);
                    }
                }
                else
                {
                    // Object has already been written, write as handle.
                    outStream.WriteByte(PortableUtils.HdrHnd);
                    outStream.WriteInt(outStartPos - hndPos);
                }

                // Synchronize input stream position.
                inStream.Seek(inStartPos + inLen, SeekOrigin.Begin);
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
        private static void WriteField(Context ctx, PortableBuilderField field)
        {
            var action = field.WriteAction;

            if (action != null)
                action(ctx.Writer, field.Value);
            else
                ctx.Writer.Write(field.Value);
        }

        /// <summary>
        /// Process portable object inverting handles if needed.
        /// </summary>
        /// <param name="outStream">Output stream.</param>
        /// <param name="port">Portable.</param>
        internal void ProcessPortable(IPortableStream outStream, PortableUserObject port)
        {
            // Special case: writing portable object with correct inversions.
            PortableHeapStream inStream = new PortableHeapStream(port.Data);

            inStream.Seek(port.Offset, SeekOrigin.Begin);

            // Use fresh context to ensure correct portable inversion.
            Mutate0(new Context(), inStream, outStream, false, 0, EmptyVals);
        }

        /// <summary>
        /// Process child builder.
        /// </summary>
        /// <param name="outStream">Output stream.</param>
        /// <param name="builder">Builder.</param>
        internal void ProcessBuilder(IPortableStream outStream, PortableBuilderImpl builder)
        {
            PortableHeapStream inStream = new PortableHeapStream(builder._obj.Data);

            inStream.Seek(builder._obj.Offset, SeekOrigin.Begin);

            // Builder parent context might be null only in one case: if we never met this group of
            // builders before. In this case we set context to their parent and track it. Context
            // cleanup will be performed at the very end of build process.
            if (builder._parent._ctx == null || builder._parent._ctx.Closed)
                builder._parent._ctx = new Context(_parent._ctx);

            builder.Mutate(inStream, outStream as PortableHeapStream, builder._desc,
                    builder._hashCode, builder._vals);
        }

        /// <summary>
        /// Write object as a predefined type if possible.
        /// </summary>
        /// <param name="hdr">Header.</param>
        /// <param name="inStream">Input stream.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="ctx">Context.</param>
        /// <returns><c>True</c> if was written.</returns>
        private bool WriteAsPredefined(byte hdr, PortableHeapStream inStream, IPortableStream outStream,
            Context ctx)
        {
            switch (hdr)
            {
                case PortableUtils.TypeByte:
                    TransferBytes(inStream, outStream, 1);

                    break;

                case PortableUtils.TypeShort:
                    TransferBytes(inStream, outStream, 2);

                    break;

                case PortableUtils.TypeInt:
                    TransferBytes(inStream, outStream, 4);

                    break;

                case PortableUtils.TypeLong:
                    TransferBytes(inStream, outStream, 8);

                    break;

                case PortableUtils.TypeFloat:
                    TransferBytes(inStream, outStream, 4);

                    break;

                case PortableUtils.TypeDouble:
                    TransferBytes(inStream, outStream, 8);

                    break;

                case PortableUtils.TypeChar:
                    TransferBytes(inStream, outStream, 2);

                    break;

                case PortableUtils.TypeBool:
                    TransferBytes(inStream, outStream, 1);

                    break;

                case PortableUtils.TypeDecimal:
                    TransferBytes(inStream, outStream, 4); // Transfer scale

                    int magLen = inStream.ReadInt(); // Transfer magnitude length.

                    outStream.WriteInt(magLen);

                    TransferBytes(inStream, outStream, magLen); // Transfer magnitude.

                    break;

                case PortableUtils.TypeString:
                    PortableUtils.WriteString(PortableUtils.ReadString(inStream), outStream);

                    break;

                case PortableUtils.TypeGuid:
                    TransferBytes(inStream, outStream, 16);

                    break;

                case PortableUtils.TypeTimestamp:
                    TransferBytes(inStream, outStream, 12);

                    break;

                case PortableUtils.TypeArrayByte:
                    TransferArray(inStream, outStream, 1);

                    break;

                case PortableUtils.TypeArrayShort:
                    TransferArray(inStream, outStream, 2);

                    break;

                case PortableUtils.TypeArrayInt:
                    TransferArray(inStream, outStream, 4);

                    break;

                case PortableUtils.TypeArrayLong:
                    TransferArray(inStream, outStream, 8);

                    break;

                case PortableUtils.TypeArrayFloat:
                    TransferArray(inStream, outStream, 4);

                    break;

                case PortableUtils.TypeArrayDouble:
                    TransferArray(inStream, outStream, 8);

                    break;

                case PortableUtils.TypeArrayChar:
                    TransferArray(inStream, outStream, 2);

                    break;

                case PortableUtils.TypeArrayBool:
                    TransferArray(inStream, outStream, 1);

                    break;

                case PortableUtils.TypeArrayDecimal:
                case PortableUtils.TypeArrayString:
                case PortableUtils.TypeArrayGuid:
                case PortableUtils.TypeArrayTimestamp:
                case PortableUtils.TypeArrayEnum:
                case PortableUtils.TypeArray:
                    int arrLen = inStream.ReadInt();

                    outStream.WriteInt(arrLen);

                    for (int i = 0; i < arrLen; i++)
                        Mutate0(ctx, inStream, outStream, false, 0, null);

                    break;

                case PortableUtils.TypeCollection:
                    int colLen = inStream.ReadInt();

                    outStream.WriteInt(colLen);

                    outStream.WriteByte(inStream.ReadByte());

                    for (int i = 0; i < colLen; i++)
                        Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);

                    break;

                case PortableUtils.TypeDictionary:
                    int dictLen = inStream.ReadInt();

                    outStream.WriteInt(dictLen);

                    outStream.WriteByte(inStream.ReadByte());

                    for (int i = 0; i < dictLen; i++)
                    {
                        Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);
                        Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);
                    }

                    break;

                case PortableUtils.TypeMapEntry:
                    Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);
                    Mutate0(ctx, inStream, outStream, false, 0, EmptyVals);

                    break;

                case PortableUtils.TypePortable:
                    TransferArray(inStream, outStream, 1); // Data array.
                    TransferBytes(inStream, outStream, 4); // Offset in array.

                    break;

                case PortableUtils.TypeEnum:
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
        private static void TransferBytes(PortableHeapStream inStream, IPortableStream outStream, int cnt)
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
        private static void TransferArray(PortableHeapStream inStream, IPortableStream outStream,
            int elemSize)
        {
            int len = inStream.ReadInt();

            outStream.WriteInt(len);

            TransferBytes(inStream, outStream, elemSize * len);
        }

        /// <summary>
        /// Mutation ocntext.
        /// </summary>
        private class Context
        {
            /** Map from object position in old portable to position in new portable. */
            private IDictionary<int, int> _oldToNew;

            /** Parent context. */
            private readonly Context _parent;

            /** Portable writer. */
            private readonly PortableWriterImpl _writer;

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
            public Context(PortableWriterImpl writer)
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
            public PortableWriterImpl Writer
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
