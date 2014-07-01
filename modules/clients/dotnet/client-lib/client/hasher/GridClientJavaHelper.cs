/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Hasher {
    using System;
    using System.Security.Cryptography;

    using Dbg = System.Diagnostics.Debug;
    using U = GridGain.Client.Util.GridClientUtils;

    /** <summary>Java hash codes calculator.</summary> */
    public class GridClientJavaHelper {
        /** 
         * <summary>
         * Calculate Java hash code for the passed object.</summary>
         * 
         * <param name="val">Object to calculate Java hash code for.</param>
         * <returns>Java hash code for passed object.</returns>
         */
        public static int GetJavaHashCode(Object val) {
            return GetJavaHashCode(val, false);
        }

        /** 
         * <summary>
         * Calculate Java hash code for the passed object.</summary>
         * 
         * <param name="val">Object to calculate Java hash code for.</param>
         *  <param name="anyType">If any type is allowed.</param>
         * <returns>Java hash code for passed object.</returns>
         */
        public static int GetJavaHashCode(Object val, bool anyType)
        {
            if (val == null)
                return 0;

            if (val is bool)
                return (bool)val ? 1 : 0;

            if (val is byte)
                return (byte)val;

            if (val is sbyte)
                return (sbyte)val;

            // Bytes order (from lowest to highest) is specific only for consistent hash.
            // So we use standart bit converter instead of GridClientUtils byte converters.

            if (val is char)
                return (char)val;

            if (val is short)
                return (short)val;

            if (val is ushort)
                return (ushort)val;

            if (val is int)
                return (int)val;

            if (val is uint)
                return (int)(uint)val;

            // Should be checked BEFORE double.
            if (val is float)
                val = Convert.ToDouble(val);

            // Should be checked BEFORE long.
            if (val is double)
                val = BitConverter.DoubleToInt64Bits((double)val);

            if (val is long)
                return (int)(((long)val) ^ (((long)val) >> 32));

            if (val is ulong)
                return (int)(((ulong)val) ^ (((ulong)val) >> 32));

            if (val is Guid)
                return HashCodeForGuid((Guid)val);

            String str = val as String;

            if (str != null)
                return HashCodeForString(str);

            var hashObj = val as IGridClientConsistentHashObject;

            if (hashObj != null)
                return hashObj.GetHashCode();

            if (anyType)
                return val.GetHashCode();
            else
                throw new InvalidOperationException("Unsupported value (does object implement " + 
                    "GridCLientConsistentHashObject?) [obj=" + val + ", type=" + val.GetType() + "]");
        }

        /** <inheritdoc /> */
        override public String ToString() {
            return "Java Hasher.";
        }

        /** 
         * <summary>Calculate Java hash code for Guid object.</summary> 
         * 
         * <param name="val">Guid object to calculate Java hash code for.</param>
         * <returns>Java hash code for passed Guid object.</returns>
         */
        private static int HashCodeForGuid(Guid val) {
            var bytes = U.ToBytes((Guid)val);
            int hash = 0;

            Dbg.Assert(bytes.Length == 16, "Expect UUID bytes representation has 16 bytes");

            for (int i = 0; i < 16; i += 4)
                hash ^= U.BytesToInt32(bytes, i);
            
            return hash;
        }

        /** 
         * <summary>Calculate Java hash code for the string.</summary> 
         * 
         * <param name="str">The string to calculate Java hash code for.</param>
         * <returns>Java hash code for the passed string.</returns>
         */
        private static int HashCodeForString(String str) {
            int h = 0;

            for (int i = 0, len = str.Length; i < len; i++)
                h = 31 * h + (int) str[i];

            return h;
        }
    }
}
