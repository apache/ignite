// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;
    using System.Text;
    using System.Collections;

    using A = GridGain.Client.Util.GridClientArgumentCheck;

    /** <summary>Generic cache request.</summary> */
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
        public IDictionary Values {
            get;
            set;
        }

        /** <summary>Keys collection.</summary> */
        public IEnumerable Keys {
            get {
                return Values.Keys;
            }

            set {
                var vals = new Hashtable();

                foreach (Object k in value)
                    vals.Add(k, null);

                Values = vals;
            }
        }
    }
}