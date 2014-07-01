/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.IO;
    using GridGain.Client.Impl.Portable;
    using GridGain.Client.Portable;
    using GridGain.Client.Util;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;
    using PSH = GridGain.Client.Impl.Portable.GridClientPortableSystemHandlers;
    
    /**
     * <summary>Portable object implementation.</summary>
     */ 
    [GridClientPortableId(PU.TYPE_PORTABLE)]
    internal class GridClientPortableObjectImpl : IGridClientPortableObject, IGridClientPortable
    {
        /** Empty fields collection. */
        private static readonly IDictionary<int, int> EMPTY_FIELDS = 
            new GridClientReadOnlyDictionary<int, int>(new Dictionary<int, int>());

        /** Raw data of this portable object. */
        private byte[] data;

        /** Offset in data array. */
        private int offset;

        /** Predefined value. */
        private object val;

        /** Marshaller. */
        private GridClientPortableMarshaller marsh;
        
        /** User type. */
        private bool userType;

        /** Type ID. */
        private int typeId;

        /** Hash code. */
        private int hashCode;

        /** Data length. */
        private int len;

        /** Raw data offset. */
        private int rawDataOffset;

        /** Fields. */
        private IDictionary<int, int> fields;

        /** Whether object can be detached form context. */
        private bool detachable;

        /**
         * <summary>Constructor.</summary>
         * <param name="marsh">Marshaller.</param>
         * <param name="data">Data bytes.</param>
         * <param name="offset">Offset.</param>
         * <param name="val">Value.</param>
         * <param name="len">Length.</param>
         * <param name="userType">User type flag.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="hashCode">Hash code.</param>
         * <param name="rawDataOffset">Raw data offset.</param>
         * <param name="fields">Fields.</param>
         * <param name="detachable">Detachable.</param>
         */
        public GridClientPortableObjectImpl(GridClientPortableMarshaller marsh, byte[] data, int offset, object val,
            int len, bool userType, int typeId, int hashCode, int rawDataOffset, IDictionary<int, int> fields, 
            bool detachable)
        {
            this.marsh = marsh;

            this.data = data;
            this.offset = offset;
            this.len = len;
            this.val = val;

            this.userType = userType;
            this.typeId = typeId;
            this.hashCode = hashCode;            
            this.rawDataOffset = rawDataOffset;

            this.fields = fields == null ? EMPTY_FIELDS : new GridClientReadOnlyDictionary<int, int>(fields);

            this.detachable = detachable;
        }
        
        /**
         * <summary>Data.</summary>
         */
        public byte[] Data
        {
            get { return data; }
        }

        /**
         * <summary>Offset.</summary>
         */
        public int Offset
        {
            get { return offset; }
        }

        /**
         * <summary>Deserialized value (set only for predefined types).</summary>
         */
        public object Value
        {
            get { return val; }
            set { val = value; }
        }

        /** <inheritdoc /> */
        public bool IsUserType()
        {
            return userType;
        }

        /**
         * <summary>Set user type flag.</summary>
         * <param name="userType">User type.</param>
         */
        public void UserType(bool userType)
        {
            this.userType = userType;
        } 

        /** <inheritdoc /> */
        public int TypeId()
        {
            return typeId;
        }

        /**
         * <summary>Set type ID.</summary>
         * <param name="typeId">Type ID.</param>
         */ 
        public void TypeId(int typeId)
        {
            this.typeId = typeId;
        } 

        /** <inheritdoc /> */
        public int HashCode()
        {
            return hashCode; 
        }

        /**
         * <summary>Set hash code.</summary>
         * <param name="hashCode">Hash code.</param>
         */
        public void HashCode(int hashCode)
        {
            this.hashCode = hashCode;
        } 

        /**
         * <summary>Length.</summary>
         */
        public int Length
        {
            get { return len; }
            set { len = value; }
        }

        /**
         * <summary>Raw data offset.</summary>
         */
        public int RawDataOffset
        {
            get { return rawDataOffset; }
            set { rawDataOffset = value; }
        }

        /**
         * <summary>Fields.</summary>
         */
        public IDictionary<int, int> Fields
        {
            set { fields = value == null ? EMPTY_FIELDS : value is GridClientReadOnlyDictionary<int, int> ? 
                value : new GridClientReadOnlyDictionary<int, int>(value); }
        }

        /**
         * <summary>Detachable flag.</summary>
         */
        public bool Detachable
        {
            set { detachable = value; }
        }

        /** <inheritdoc /> */
        public T Field<T>(string fieldName)
        {
            if (userType) {
                GridClientPortableTypeDescriptor desc;

                if (marsh.IdToDescriptor.TryGetValue(PU.TypeKey(true, typeId), out desc))
                {
                    int? fieldIdRef = desc.Mapper.FieldId(typeId, fieldName);

                    int fieldId = fieldIdRef.HasValue ? fieldIdRef.Value : (PU.StringHashCode(fieldName.ToLower()));

                    int pos;

                    return fields.TryGetValue(fieldId, out pos) ? Field0<T>(offset + pos) : default(T);
                }
                else
                    throw new GridClientPortableException("Unknown user type: " + typeId);
                    
            }
            else 
                throw new GridClientPortableException("Cannot get field by name on system type: " + typeId);
        }

        /**
         * <summary>Gets field value on the given.</summary>
         * <param name="pos">Position.</param>
         * <returns>Field value.</returns>
         */ 
        private T Field0<T>(int pos)
        {
            // 1. Read field length.
            MemoryStream stream = new MemoryStream(data);

            stream.Seek(pos + 4, SeekOrigin.Begin); // Skip 4 additional bytes (field ID).

            int len = PU.ReadInt(stream);

            byte hdr = PU.ReadByte(stream);

            if (hdr == PU.HDR_NULL)
                return default(T);
            else if (hdr == PU.HDR_HND)
                return (T)marsh.Unmarshal0(stream, false, stream.Position - 1, PU.HDR_HND);
            else if (hdr == PU.HDR_FULL || PU.IsPredefinedType(hdr))                
            {
                IGridClientPortableObject obj = marsh.Unmarshal0(stream, false, stream.Position - 1, hdr);

                return PU.PortableOrPredefined<T>(obj);
            }
            else
            {
                Debug.Assert(userType == false);

                GridClientPortableSystemFieldDelegate hnd = PSH.FieldHandler(hdr);

                if (hnd == null)
                    throw new GridClientPortableException("Invalid system type: " + hdr);

                return (T)hnd.Invoke(stream, marsh);
            }
        }
             
        /** <inheritdoc /> */
        public unsafe T Deserialize<T>()
        {
            if (val != null) {
                // 1. Handle special conversions first.
                if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(sbyte?))
                {
                    byte val0 = (byte)val;

                    return (T)(object)(*(sbyte*)&val0);
                }
                else if (typeof(T) == typeof(ushort) || typeof(T) == typeof(ushort?))
                {
                    short val0 = (short)val;

                    return (T)(object)(*(ushort*)&val0);
                }
                else if (typeof(T) == typeof(uint) || typeof(T) == typeof(uint?))
                {
                    int val0 = (int)val;

                    return (T)(object)(*(uint*)&val0);
                }
                else if (typeof(T) == typeof(ulong) || typeof(T) == typeof(ulong?))
                {
                    long val0 = (long)val;

                    return (T)(object)(*(ulong*)&val0);
                }
                else if (typeof(T) == typeof(sbyte[])) 
                {
                    byte[] val0 = (byte[])val;

                    sbyte[] res = new sbyte[val0.Length];

                    for (int i = 0; i < val0.Length; i++)
                    {
                        byte curVal = val0[i];

                        res[i] = *(sbyte*)&curVal;
                    }

                    return (T)(object)res;
                }
                else if (typeof(T) == typeof(ushort[]))
                {
                    short[] val0 = (short[])val;

                    ushort[] res = new ushort[val0.Length];

                    for (int i = 0; i < val0.Length; i++)
                    {
                        short curVal = val0[i];

                        res[i] = *(ushort*)&curVal;
                    }

                    return (T)(object)res;
                }
                else if (typeof(T) == typeof(uint[]))
                {
                    int[] val0 = (int[])val;

                    uint[] res = new uint[val0.Length];

                    for (int i = 0; i < val0.Length; i++)
                    {
                        int curVal = val0[i];

                        res[i] = *(uint*)&curVal;
                    }

                    return (T)(object)res;
                }
                else if (typeof(T) == typeof(ulong[]))
                {
                    long[] val0 = (long[])val;

                    ulong[] res = new ulong[val0.Length];

                    for (int i = 0; i < val0.Length; i++)
                    {
                        long curVal = val0[i];

                        res[i] = *(ulong*)&curVal;
                    }

                    return (T)(object)res;
                }

                // 2. Nothing special, so regular conversion.
                return (T)val;
            }
            else
            {
                // 3. Full deserialization.
                MemoryStream stream = new MemoryStream(data);

                stream.Position = offset;

                return new GridClientPortableReadContext(marsh, marsh.IdToDescriptor, stream).Deserialize<T>(this);
            }
        }

        /** <inheritdoc /> */
        public IGridClientPortableObject Copy(IDictionary<string, object> fields)
        {
            throw new NotImplementedException();
        }

        /**
         * <summary>Gets position of the given field ID.</summary>
         * <param name="fieldId">Field ID.</param>
         * <returns>Position.</returns>
         */
        public int? Position(int fieldId)
        {
            int pos;

            return fields.TryGetValue(fieldId, out pos) ? pos : (int?)null;
        }

        /**
         * <summary>Populates portable object with data.</summary>
         * <param name="marsh">Marshaller.</param>
         */ 
        public void Populate(GridClientPortableMarshaller marsh)
        {
            this.marsh = marsh;

            marsh.PreparePortable(this);
        }

        /**
         * <summary>Detach object from context.</summary>
         */ 
        public GridClientPortableObjectImpl Detach()
        {
            if (detachable)
            {
                byte[] data0 = new byte[len];

                Array.Copy(data, offset, data0, 0, len);

                return new GridClientPortableObjectImpl(marsh, data0, 0, val, len, userType, typeId, 
                    hashCode, rawDataOffset, fields, false);
            }
            else
                return this;
        }

        /** <inheritdoc /> */
        public void WritePortable(IGridClientPortableWriter writer)
        {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteByteArray(data);
            rawWriter.WriteInt(offset);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IGridClientPortableReader reader)
        {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            data = rawReader.ReadByteArray();
            offset = rawReader.ReadInt();
        }
    }
}
