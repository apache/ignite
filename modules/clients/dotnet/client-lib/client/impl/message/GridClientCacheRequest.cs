/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System.Collections.Generic;

namespace GridGain.Client.Impl.Message {
    using System;
    using System.Text;
    using System.Collections;
    using GridGain.Client.Impl.Portable;
    using GridGain.Client.Portable;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary>Generic cache request.</summary> */
    [GridClientPortableId(PU.TYPE_CACHE_REQ)]
    internal class GridClientCacheRequest : GridClientRequest {
        /**
         * <summary>
         * Tries to find enum value by operation code.</summary>
         *
         * <param name="val">Operation code value.</param>
         * <returns>Enum value.</returns>
         */
        public static GridClientCacheRequestOperation FindByCode(int val) {
            foreach (GridClientCacheRequestOperation op in Enum.GetValues(typeof(GridClientCacheRequestOperation)))
                if (val == (int)op)
                    return op;

            throw new ArgumentException("Invalid cache operation code: " + val);
        }

        /**
         * <summary>
         * Creates grid cache request.</summary>
         *
         * <param name="op">Requested operation.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         */
        public GridClientCacheRequest(GridClientCacheRequestOperation op, Guid destNodeId)
            : base(destNodeId) {
            this.Operation = op;
        }

        /** <summary>Requested cache operation.</summary> */
        public GridClientCacheRequestOperation Operation {
            get;
            private set;
        }

        /** <summary>Cache name.</summary> */
        public String CacheName {
            get;
            set;
        }

        /** <summary>Cache flags.</summary>*/
        public int CacheFlags;

        /** <summary>Key.</summary> */
        public Object Key {
            get;
            set;
        }

        /** <summary>Value (expected value for CAS).</summary> */
        public Object Value {
            get;
            set;
        }

        /** <summary>New value for CAS.</summary> */
        public Object Value2 {
            get;
            set;
        }

        /** <summary>Keys and values for put all, get all, remove all operations.</summary> */
        public IDictionary<Object, Object> Values {
            get;
            set;
        }

        /** <summary>Keys collection.</summary> */
        public IEnumerable Keys {
            get {
                return Values.Keys;
            }

            set {
                var vals = new Dictionary<object, object>();

                foreach (Object k in value)
                    vals.Add(k, null);

                Values = vals;
            }
        }

        /** <inheritdoc /> */
        public override void WritePortable(IGridClientPortableWriter writer) {
            base.WritePortable(writer);

            GridClientPortableWriterImpl rawWriter = (GridClientPortableWriterImpl)writer.RawWriter();

            rawWriter.WriteInt((int)Operation);
            rawWriter.WriteString(CacheName);
            rawWriter.WriteInt(CacheFlags);

            rawWriter.DetachNext();
            rawWriter.WriteObject(Key);

            rawWriter.DetachNext();
            rawWriter.WriteObject(Value);

            rawWriter.DetachNext();
            rawWriter.WriteObject(Value2);

            rawWriter.WriteInt(Values != null ? Values.Count : -1);

            if (Values != null)
            {
                foreach (KeyValuePair<object, object> pair in Values)
                {
                    rawWriter.DetachNext();
                    rawWriter.WriteObject<object>(pair.Key);

                    rawWriter.DetachNext();
                    rawWriter.WriteObject<object>(pair.Value);
                }
            }
        }

        /** <inheritdoc /> */
        public override void ReadPortable(IGridClientPortableReader reader) {
            base.ReadPortable(reader);

            GridClientPortableReaderImpl rawReader = (GridClientPortableReaderImpl)reader.RawReader();

            Operation = (GridClientCacheRequestOperation)rawReader.ReadInt();
            CacheName = rawReader.ReadString();
            CacheFlags = rawReader.ReadInt();

            rawReader.DetachNext();
            Key = PU.PortableOrPredefined<object>(rawReader.ReadPortable());

            rawReader.DetachNext();
            Value = PU.PortableOrPredefined<object>(rawReader.ReadPortable());

            rawReader.DetachNext();
            Value2 = PU.PortableOrPredefined<object>(rawReader.ReadPortable());

            int valsCnt = rawReader.ReadInt();

            if (valsCnt >= 0)
            {
                Values = new Dictionary<object, object>(valsCnt);

                for (int i = 0; i < valsCnt; i++)
                {
                    rawReader.DetachNext();
                    object key = PU.PortableOrPredefined<object>(rawReader.ReadPortable());

                    rawReader.DetachNext();
                    object val = PU.PortableOrPredefined<object>(rawReader.ReadPortable());

                    Values[key] = val;
                }
            }
        }
    }
}
