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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Text;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;

    /// <summary>
    /// Native utility methods.
    /// </summary>
    internal static class IgniteUtils
    {
        /** Thread-local random. */
        [ThreadStatic]
        private static Random _rnd;

        /// <summary>
        /// Gets thread local random.
        /// </summary>
        /// <value>Thread local random.</value>
        public static Random ThreadLocalRandom
        {
            get { return _rnd ?? (_rnd = new Random()); }
        }

        /// <summary>
        /// Returns shuffled list copy.
        /// </summary>
        /// <returns>Shuffled list copy.</returns>
        public static IList<T> Shuffle<T>(IList<T> list)
        {
            int cnt = list.Count;

            if (cnt > 1) {
                List<T> res = new List<T>(list);

                Random rnd = ThreadLocalRandom;

                while (cnt > 1)
                {
                    cnt--;
                    
                    int idx = rnd.Next(cnt + 1);

                    T val = res[idx];
                    res[idx] = res[cnt];
                    res[cnt] = val;
                }

                return res;
            }
            return list;
        }

        /// <summary>
        /// Create new instance of specified class.
        /// </summary>
        /// <param name="typeName">Class name</param>
        /// <param name="props">Properties to set.</param>
        /// <returns>New Instance.</returns>
        public static T CreateInstance<T>(string typeName, IEnumerable<KeyValuePair<string, object>> props = null)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            var type = new TypeResolver().ResolveType(typeName);

            if (type == null)
                throw new IgniteException("Failed to create class instance [className=" + typeName + ']');

            var res =  (T) Activator.CreateInstance(type);

            if (props != null)
                SetProperties(res, props);

            return res;
        }

        /// <summary>
        /// Set properties on the object.
        /// </summary>
        /// <param name="target">Target object.</param>
        /// <param name="props">Properties.</param>
        private static void SetProperties(object target, IEnumerable<KeyValuePair<string, object>> props)
        {
            if (props == null)
                return;

            IgniteArgumentCheck.NotNull(target, "target");

            Type typ = target.GetType();

            foreach (KeyValuePair<string, object> prop in props)
            {
                PropertyInfo prop0 = typ.GetProperty(prop.Key, 
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

                if (prop0 == null)
                    throw new IgniteException("Property is not found [type=" + typ.Name + 
                        ", property=" + prop.Key + ']');

                prop0.SetValue(target, prop.Value, null);
            }
        }

        /// <summary>
        /// Convert unmanaged char array to string.
        /// </summary>
        /// <param name="chars">Char array.</param>
        /// <param name="charsLen">Char array length.</param>
        /// <returns></returns>
        public static unsafe string Utf8UnmanagedToString(sbyte* chars, int charsLen)
        {
            IntPtr ptr = new IntPtr(chars);

            if (ptr == IntPtr.Zero)
                return null;

            byte[] arr = new byte[charsLen];

            Marshal.Copy(ptr, arr, 0, arr.Length);

            return Encoding.UTF8.GetString(arr);
        }

        /// <summary>
        /// Convert string to unmanaged byte array.
        /// </summary>
        /// <param name="str">String.</param>
        /// <returns>Unmanaged byte array.</returns>
        public static unsafe sbyte* StringToUtf8Unmanaged(string str)
        {
            var ptr = IntPtr.Zero;

            if (str != null)
            {
                byte[] strBytes = Encoding.UTF8.GetBytes(str);

                ptr = Marshal.AllocHGlobal(strBytes.Length + 1);

                Marshal.Copy(strBytes, 0, ptr, strBytes.Length);

                *((byte*)ptr.ToPointer() + strBytes.Length) = 0; // NULL-terminator.
            }
            
            return (sbyte*)ptr.ToPointer();
        }

        /// <summary>
        /// Reads node collection from stream.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="pred">The predicate.</param>
        /// <returns> Nodes list or null. </returns>
        public static List<IClusterNode> ReadNodes(BinaryReader reader, Func<ClusterNodeImpl, bool> pred = null)
        {
            var cnt = reader.ReadInt();

            if (cnt < 0)
                return null;

            var res = new List<IClusterNode>(cnt);

            var ignite = reader.Marshaller.Ignite;

            if (pred == null)
            {
                for (var i = 0; i < cnt; i++)
                    res.Add(ignite.GetNode(reader.ReadGuid()));
            }
            else
            {
                for (var i = 0; i < cnt; i++)
                {
                    var node = ignite.GetNode(reader.ReadGuid());
                    
                    if (pred(node))
                        res.Add(node);
                }
            }

            return res;
        }

        /// <summary>
        /// Encodes the peek modes into a single int value.
        /// </summary>
        public static int EncodePeekModes(CachePeekMode[] modes, out bool hasPlatformCache)
        {
            var res = 0;
            hasPlatformCache = false;

            if (modes == null)
            {
                return res;
            }

            foreach (var mode in modes)
            {
                res |= (int) mode;
            }

            // Clear Platform bit: Java does not understand it.
            const int platformCache = (int) CachePeekMode.Platform;
            const int all = (int) CachePeekMode.All;
            hasPlatformCache = (res & platformCache) == platformCache || (res & all) == all;
            
            return res & ~platformCache;
        }
    }
}
