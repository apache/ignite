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
    using System.Collections.Generic;
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
        /** Type IDs for metadata. */
        private static readonly IDictionary<Type, int> TYPE_IDS;

        /** Cached dictionary with no values. */
        private static readonly IDictionary<int, object> EMPTY_VALS = new Dictionary<int, object>();

        /** Offset: length. */
        private const int OFFSET_LEN = 10;

        /** Portables. */
        private readonly PortablesImpl portables;

        /** */
        private readonly PortableBuilderImpl parent;

        /** Initial portable object. */
        private readonly PortableUserObject obj;

        /** Type descriptor. */
        private readonly IPortableTypeDescriptor desc;

        /** Values. */
        private IDictionary<string, PortableBuilderField> vals;

        /** Contextual fields. */
        private IDictionary<int, object> cache;

        /** Hash code. */
        private int hashCode;
        
        /** Current context. */
        private Context ctx;
        
        /// <summary>
        /// Static initializer.
        /// </summary>
        static PortableBuilderImpl()
        {
            TYPE_IDS = new Dictionary<Type, int>();

            // 1. Primitives.
            TYPE_IDS[typeof(byte)] = PortableUtils.TYPE_BYTE;
            TYPE_IDS[typeof(bool)] = PortableUtils.TYPE_BOOL;
            TYPE_IDS[typeof(short)] = PortableUtils.TYPE_SHORT;
            TYPE_IDS[typeof(char)] = PortableUtils.TYPE_CHAR;
            TYPE_IDS[typeof(int)] = PortableUtils.TYPE_INT;
            TYPE_IDS[typeof(long)] = PortableUtils.TYPE_LONG;
            TYPE_IDS[typeof(float)] = PortableUtils.TYPE_FLOAT;
            TYPE_IDS[typeof(double)] = PortableUtils.TYPE_DOUBLE;
            TYPE_IDS[typeof(decimal)] = PortableUtils.TYPE_DECIMAL;

            TYPE_IDS[typeof(byte[])] = PortableUtils.TYPE_ARRAY_BYTE;
            TYPE_IDS[typeof(bool[])] = PortableUtils.TYPE_ARRAY_BOOL;
            TYPE_IDS[typeof(short[])] = PortableUtils.TYPE_ARRAY_SHORT;
            TYPE_IDS[typeof(char[])] = PortableUtils.TYPE_ARRAY_CHAR;
            TYPE_IDS[typeof(int[])] = PortableUtils.TYPE_ARRAY_INT;
            TYPE_IDS[typeof(long[])] = PortableUtils.TYPE_ARRAY_LONG;
            TYPE_IDS[typeof(float[])] = PortableUtils.TYPE_ARRAY_FLOAT;
            TYPE_IDS[typeof(double[])] = PortableUtils.TYPE_ARRAY_DOUBLE;
            TYPE_IDS[typeof(decimal[])] = PortableUtils.TYPE_ARRAY_DECIMAL;

            // 2. String.
            TYPE_IDS[typeof(string)] = PortableUtils.TYPE_STRING;
            TYPE_IDS[typeof(string[])] = PortableUtils.TYPE_ARRAY_STRING;

            // 3. Guid.
            TYPE_IDS[typeof(Guid)] = PortableUtils.TYPE_GUID;
            TYPE_IDS[typeof(Guid?)] = PortableUtils.TYPE_GUID;
            TYPE_IDS[typeof(Guid[])] = PortableUtils.TYPE_ARRAY_GUID;
            TYPE_IDS[typeof(Guid?[])] = PortableUtils.TYPE_ARRAY_GUID;

            // 4. Date.
            TYPE_IDS[typeof(DateTime)] = PortableUtils.TYPE_DATE;
            TYPE_IDS[typeof(DateTime?)] = PortableUtils.TYPE_DATE;
            TYPE_IDS[typeof(DateTime[])] = PortableUtils.TYPE_ARRAY_DATE;
            TYPE_IDS[typeof(DateTime?[])] = PortableUtils.TYPE_ARRAY_DATE;
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="portables">Portables.</param>
        /// <param name="obj">Initial portable object.</param>
        /// <param name="desc">Type descriptor.</param>
        public PortableBuilderImpl(PortablesImpl portables, PortableUserObject obj,
            IPortableTypeDescriptor desc) : this(portables, null, obj, desc) 
        { 
            // No-op.
        }

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
            this.portables = portables;
            this.parent = parent ?? this;
            this.obj = obj;
            this.desc = desc;

            hashCode = obj.GetHashCode();
        }

        /** <inheritDoc /> */
        public IPortableBuilder HashCode(int hashCode)
        {
            this.hashCode = hashCode;

            return this;
        }

        /** <inheritDoc /> */
        public T GetField<T>(string name)
        {
            PortableBuilderField field;

            if (vals != null && vals.TryGetValue(name, out field))
                return field != PortableBuilderField.RMV_MARKER ? (T)field.Value : default(T);
            else
            {
                T val = obj.Field<T>(name, this);

                if (vals == null)
                    vals = new Dictionary<string, PortableBuilderField>(2);

                vals[name] = new PortableBuilderField(typeof(T), val);

                return val;
            }
        }

        /** <inheritDoc /> */
        public IPortableBuilder SetField<T>(string name, T val)
        {
            return SetField0(name, new PortableBuilderField(typeof(T), val));
        }

        /** <inheritDoc /> */
        public IPortableBuilder RemoveField(string name)
        {
            return SetField0(name, PortableBuilderField.RMV_MARKER);
        }

        /** <inheritDoc /> */
        public IPortableObject Build()
        {
            PortableHeapStream inStream = new PortableHeapStream(obj.Data);

            inStream.Seek(obj.Offset, SeekOrigin.Begin);

            // Assume that resulting length will be no less than header + [fields_cnt] * 12;
            int len = PortableUtils.FULL_HDR_LEN + (vals == null ? 0 : vals.Count * 12);

            PortableHeapStream outStream = new PortableHeapStream(len);

            var writer = portables.Marshaller.StartMarshal(outStream);

            writer.SetBuilder(this);

            // All related builders will work in this context with this writer.
            parent.ctx = new Context(writer);
            
            try
            {
                // Write.
                writer.Write(this);
                
                // Process metadata.
                portables.Marshaller.FinishMarshal(writer);

                // Create portable object once metadata is processed.
                return new PortableUserObject(portables.Marshaller, outStream.InternalArray, 0,
                    desc.TypeId, hashCode);
            }
            finally
            {
                // Cleanup.
                parent.ctx.Closed = true;
            }
        }

        /// <summary>
        /// Create child builder.
        /// </summary>
        /// <param name="obj">Portable object.</param>
        /// <returns>Child builder.</returns>
        public PortableBuilderImpl Child(PortableUserObject obj)
        {
            return portables.ChildBuilder(parent, obj);
        }
        
        /// <summary>
        /// Get cache field.
        /// </summary>
        /// <param name="pos">Position.</param>
        /// <param name="val">Value.</param>
        /// <returns><c>true</c> if value is found in cache.</returns>
        public bool CachedField<T>(int pos, out T val)
        {
            if (parent.cache != null)
            {
                object res;

                if (parent.cache.TryGetValue(pos, out res))
                {
                    val = res != null ? (T)res : default(T);

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
        public void CacheField(int pos, object val)
        {
            if (parent.cache == null)
                parent.cache = new Dictionary<int, object>(2);

            parent.cache[pos] = val;
        }

        /// <summary>
        /// Internal set field routine.
        /// </summary>
        /// <param name="fieldName">Name.</param>
        /// <param name="val">Value.</param>
        /// <returns>This builder.</returns>
        private IPortableBuilder SetField0(string fieldName, PortableBuilderField val)
        {
            if (vals == null)
                vals = new Dictionary<string, PortableBuilderField>();

            vals[fieldName] = val;

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
        internal void Mutate(
            PortableHeapStream inStream,
            PortableHeapStream outStream,
            IPortableTypeDescriptor desc,
            int hashCode, 
            IDictionary<string, PortableBuilderField> vals)
        {
            // Set correct builder to writer frame.
            PortableBuilderImpl oldBuilder = parent.ctx.Writer.SetBuilder(parent);

            int streamPos = inStream.Position;
            
            try
            {
                // Prepare fields.
                IPortableMetadataHandler metaHnd = portables.Marshaller.MetadataHandler(desc);

                IDictionary<int, object> vals0;

                if (vals == null || vals.Count == 0)
                    vals0 = EMPTY_VALS;
                else
                {
                    vals0 = new Dictionary<int, object>(vals.Count);

                    foreach (KeyValuePair<string, PortableBuilderField> valEntry in vals)
                    {
                        int fieldId = PortableUtils.FieldId(desc.TypeId, valEntry.Key, desc.NameConverter, desc.Mapper);

                        if (vals0.ContainsKey(fieldId))
                            throw new IgniteException("Collision in field ID detected (change field name or " +
                                "define custom ID mapper) [fieldName=" + valEntry.Key + ", fieldId=" + fieldId + ']');

                        vals0[fieldId] = valEntry.Value.Value;

                        // Write metadata if: 1) it is enabled for type; 2) type is not null (i.e. it is neither 
                        // remove marker, nor a field read through "GetField" method.
                        if (metaHnd != null && valEntry.Value.Type != null)
                            metaHnd.OnFieldWrite(fieldId, valEntry.Key, TypeId(valEntry.Value.Type));
                    }
                }

                // Actual processing.
                Mutate0(parent.ctx, inStream, outStream, true, hashCode, vals0);

                // 3. Handle metadata.
                if (metaHnd != null)
                {
                    IDictionary<string, int> meta = metaHnd.OnObjectWriteFinished();

                    if (meta != null)
                        parent.ctx.Writer.SaveMetadata(desc.TypeId, desc.TypeName, desc.AffinityKeyFieldName, meta);
                }
            }
            finally
            {
                // Restore builder frame.
                parent.ctx.Writer.SetBuilder(oldBuilder);

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
            bool changeHash, int hash, IDictionary<int, object> vals)
        {
            int inStartPos = inStream.Position;
            int outStartPos = outStream.Position;

            byte inHdr = inStream.ReadByte();

            if (inHdr == PortableUtils.HDR_NULL)
                outStream.WriteByte(PortableUtils.HDR_NULL);
            else if (inHdr == PortableUtils.HDR_HND)
            {
                int inHnd = inStream.ReadInt();

                int oldPos = inStartPos - inHnd;
                int newPos;

                if (ctx.OldToNew(oldPos, out newPos))
                {
                    // Handle is still valid.
                    outStream.WriteByte(PortableUtils.HDR_HND);
                    outStream.WriteInt(outStartPos - newPos);
                }
                else
                {
                    // Handle is invalid, write full object.
                    int inRetPos = inStream.Position;

                    inStream.Seek(oldPos, SeekOrigin.Begin);

                    Mutate0(ctx, inStream, outStream, false, 0, EMPTY_VALS);

                    inStream.Seek(inRetPos, SeekOrigin.Begin);
                }
            }
            else if (inHdr == PortableUtils.HDR_FULL)
            {
                byte inUsrFlag = inStream.ReadByte();
                int inTypeId = inStream.ReadInt();
                int inHash = inStream.ReadInt();
                int inLen = inStream.ReadInt();
                int inRawOff = inStream.ReadInt();

                int hndPos;

                if (ctx.AddOldToNew(inStartPos, outStartPos, out hndPos))
                {
                    // Object could be cached in parent builder.
                    object cachedVal;

                    if (parent.cache != null && parent.cache.TryGetValue(inStartPos, out cachedVal)) {
                        ctx.Writer.Write(cachedVal);
                    }
                    else
                    {
                        // New object, write in full form.
                        outStream.WriteByte(PortableUtils.HDR_FULL);
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

                            object fieldVal;

                            bool fieldFound = vals.TryGetValue(inFieldId, out fieldVal);

                            if (!fieldFound || fieldVal != PortableBuilderField.RMV_MARKER_OBJ)
                            {
                                outStream.WriteInt(inFieldId);

                                int fieldLenPos = outStream.Position; // Here we will write length later.

                                outStream.Seek(4, SeekOrigin.Current);

                                if (fieldFound)
                                {
                                    // Replace field with new value.
                                    if (fieldVal != PortableBuilderField.RMV_MARKER_OBJ)
                                        ctx.Writer.Write(fieldVal);

                                    vals.Remove(inFieldId);
                                }
                                else
                                {
                                    // If field was requested earlier, then we must write tracked value
                                    if (parent.cache != null && parent.cache.TryGetValue(inFieldDataPos, out fieldVal))
                                        ctx.Writer.Write(fieldVal);
                                    else
                                        // Filed is not tracked, re-write as is.
                                        Mutate0(ctx, inStream, outStream, false, 0, EMPTY_VALS);                                    
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
                        foreach (KeyValuePair<int, object> valEntry in vals)
                        {
                            if (valEntry.Value != PortableBuilderField.RMV_MARKER_OBJ)
                            {
                                outStream.WriteInt(valEntry.Key);

                                int fieldLenPos = outStream.Position; // Here we will write length later.

                                outStream.Seek(4, SeekOrigin.Current);

                                ctx.Writer.Write(valEntry.Value);

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

                        outStream.Seek(outStartPos + OFFSET_LEN, SeekOrigin.Begin);

                        outStream.WriteInt(outResPos - outStartPos); // Length.
                        outStream.WriteInt(rawPos - outStartPos); // Raw offset.

                        outStream.Seek(outResPos, SeekOrigin.Begin);
                    }
                }
                else
                {
                    // Object has already been written, write as handle.
                    outStream.WriteByte(PortableUtils.HDR_HND);
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
            Mutate0(new Context(), inStream, outStream, false, 0, EMPTY_VALS);
        }

        /// <summary>
        /// Process child builder.
        /// </summary>
        /// <param name="outStream">Output stream.</param>
        /// <param name="builder">Builder.</param>
        internal void ProcessBuilder(IPortableStream outStream, PortableBuilderImpl builder)
        {
            PortableHeapStream inStream = new PortableHeapStream(builder.obj.Data);

            inStream.Seek(builder.obj.Offset, SeekOrigin.Begin);

            // Builder parent context might be null only in one case: if we never met this group of
            // builders before. In this case we set context to their parent and track it. Context
            // cleanup will be performed at the very end of build process.
            if (builder.parent.ctx == null || builder.parent.ctx.Closed)
                builder.parent.ctx = new Context(parent.ctx);

            builder.Mutate(inStream, outStream as PortableHeapStream, builder.desc,
                    builder.hashCode, builder.vals);
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
                case PortableUtils.TYPE_BYTE:
                    TransferBytes(inStream, outStream, 1);

                    break;

                case PortableUtils.TYPE_SHORT:
                    TransferBytes(inStream, outStream, 2);

                    break;

                case PortableUtils.TYPE_INT:
                    TransferBytes(inStream, outStream, 4);

                    break;

                case PortableUtils.TYPE_LONG:
                    TransferBytes(inStream, outStream, 8);

                    break;

                case PortableUtils.TYPE_FLOAT:
                    TransferBytes(inStream, outStream, 4);

                    break;

                case PortableUtils.TYPE_DOUBLE:
                    TransferBytes(inStream, outStream, 8);

                    break;

                case PortableUtils.TYPE_CHAR:
                    TransferBytes(inStream, outStream, 2);

                    break;

                case PortableUtils.TYPE_BOOL:
                    TransferBytes(inStream, outStream, 1);

                    break;

                case PortableUtils.TYPE_DECIMAL:
                    TransferBytes(inStream, outStream, 4); // Transfer scale

                    int magLen = inStream.ReadInt(); // Transfer magnitude length.

                    outStream.WriteInt(magLen);

                    TransferBytes(inStream, outStream, magLen); // Transfer magnitude.

                    break;

                case PortableUtils.TYPE_STRING:
                    PortableUtils.WriteString(PortableUtils.ReadString(inStream), outStream);

                    break;

                case PortableUtils.TYPE_GUID:
                    TransferBytes(inStream, outStream, 16);

                    break;

                case PortableUtils.TYPE_DATE:
                    TransferBytes(inStream, outStream, 12);

                    break;

                case PortableUtils.TYPE_ARRAY_BYTE:
                    TransferArray(inStream, outStream, 1);

                    break;

                case PortableUtils.TYPE_ARRAY_SHORT:
                    TransferArray(inStream, outStream, 2);

                    break;

                case PortableUtils.TYPE_ARRAY_INT:
                    TransferArray(inStream, outStream, 4);

                    break;

                case PortableUtils.TYPE_ARRAY_LONG:
                    TransferArray(inStream, outStream, 8);

                    break;

                case PortableUtils.TYPE_ARRAY_FLOAT:
                    TransferArray(inStream, outStream, 4);

                    break;

                case PortableUtils.TYPE_ARRAY_DOUBLE:
                    TransferArray(inStream, outStream, 8);

                    break;

                case PortableUtils.TYPE_ARRAY_CHAR:
                    TransferArray(inStream, outStream, 2);

                    break;

                case PortableUtils.TYPE_ARRAY_BOOL:
                    TransferArray(inStream, outStream, 1);

                    break;

                case PortableUtils.TYPE_ARRAY_DECIMAL:
                case PortableUtils.TYPE_ARRAY_STRING:
                case PortableUtils.TYPE_ARRAY_GUID:
                case PortableUtils.TYPE_ARRAY_DATE:
                case PortableUtils.TYPE_ARRAY_ENUM:
                case PortableUtils.TYPE_ARRAY:
                    int arrLen = inStream.ReadInt();

                    outStream.WriteInt(arrLen);

                    for (int i = 0; i < arrLen; i++)
                        Mutate0(ctx, inStream, outStream, false, 0, null);

                    break;

                case PortableUtils.TYPE_COLLECTION:
                    int colLen = inStream.ReadInt();

                    outStream.WriteInt(colLen);

                    outStream.WriteByte(inStream.ReadByte());

                    for (int i = 0; i < colLen; i++)
                        Mutate0(ctx, inStream, outStream, false, 0, EMPTY_VALS);

                    break;

                case PortableUtils.TYPE_DICTIONARY:
                    int dictLen = inStream.ReadInt();

                    outStream.WriteInt(dictLen);

                    outStream.WriteByte(inStream.ReadByte());

                    for (int i = 0; i < dictLen; i++)
                    {
                        Mutate0(ctx, inStream, outStream, false, 0, EMPTY_VALS);
                        Mutate0(ctx, inStream, outStream, false, 0, EMPTY_VALS);
                    }

                    break;

                case PortableUtils.TYPE_MAP_ENTRY:
                    Mutate0(ctx, inStream, outStream, false, 0, EMPTY_VALS);
                    Mutate0(ctx, inStream, outStream, false, 0, EMPTY_VALS);

                    break;

                case PortableUtils.TYPE_PORTABLE:
                    TransferArray(inStream, outStream, 1); // Data array.
                    TransferBytes(inStream, outStream, 4); // Offset in array.

                    break;

                case PortableUtils.TYPE_ENUM:
                    TransferBytes(inStream, outStream, 4); // Integer ordinal.

                    break;

                default:
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Get's metadata field type ID for the given type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Type ID.</returns>
        private static int TypeId(Type type)
        {
            int typeId;

            if (TYPE_IDS.TryGetValue(type, out typeId))
                return typeId;
            else if (type.IsEnum)
                return PortableUtils.TYPE_ENUM;
            else if (type.IsArray)
                return type.GetElementType().IsEnum ? PortableUtils.TYPE_ARRAY_ENUM : PortableUtils.TYPE_ARRAY;
            else
            {
                PortableCollectionInfo colInfo = PortableCollectionInfo.Info(type);

                return colInfo.IsAny ? colInfo.IsCollection || colInfo.IsGenericCollection ?
                    PortableUtils.TYPE_COLLECTION : PortableUtils.TYPE_DICTIONARY : PortableUtils.TYPE_OBJECT;
            }
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
            private IDictionary<int, int> oldToNew;

            /** Parent context. */
            private readonly Context parent;

            /** Portable writer. */
            private readonly IPortableWriterEx writer;

            /** Children contexts. */
            private ICollection<Context> children;

            /** Closed flag; if context is closed, it can no longer be used. */
            private bool closed;

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
            public Context(IPortableWriterEx writer)
            {
                this.writer = writer;
            }

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="parent">Parent context.</param>
            public Context(Context parent)
            {
                this.parent = parent;
                
                writer = parent.writer;

                if (parent.children == null)
                    parent.children = new List<Context>();

                parent.children.Add(this);
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
                if (oldToNew == null)
                    oldToNew = new Dictionary<int, int>();

                if (oldToNew.TryGetValue(oldPos, out hndPos))
                    return false;
                else
                {
                    oldToNew[oldPos] = newPos;

                    return true;
                }
            }

            /// <summary>
            /// Get mapping of old position to the new one.
            /// </summary>
            /// <param name="oldPos">Old position.</param>
            /// <param name="newPos">New position.</param>
            /// <returns><c>True</c> if mapping exists.</returns>
            public bool OldToNew(int oldPos, out int newPos)
            {
                return oldToNew.TryGetValue(oldPos, out newPos);
            }

            /// <summary>
            /// Writer.
            /// </summary>
            public IPortableWriterEx Writer
            {
                get { return writer; }
            }

            /// <summary>
            /// Closed flag.
            /// </summary>
            public bool Closed
            {
                get
                {
                    return closed;
                }
                set
                {
                    Context ctx = this;

                    while (ctx != null)
                    {
                        ctx.closed = value;

                        if (children != null) {
                            foreach (Context child in children)
                                child.Closed = value;
                        }

                        ctx = ctx.parent;
                    }
                }
            }
        }
    }
}
