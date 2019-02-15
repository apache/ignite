/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Resource;

    [SuppressMessage("ReSharper", "FieldCanBeMadeReadOnly.Local")]
    public class CacheTestStore : ICacheStore<object, object>
    {
        public static readonly IDictionary Map = new ConcurrentDictionary<object, object>();

        public static bool LoadMultithreaded;

        public static bool LoadObjects;

        public static bool ThrowError;

        [InstanceResource]
        private IIgnite _grid = null;

        [StoreSessionResource]
#pragma warning disable 649
        private ICacheStoreSession _ses;
#pragma warning restore 649

        public static int intProperty;

        public static string stringProperty;

        public static void Reset()
        {
            Map.Clear();

            LoadMultithreaded = false;
            LoadObjects = false;
            ThrowError = false;
        }

        public void LoadCache(Action<object, object> act, params object[] args)
        {
            ThrowIfNeeded();

            Debug.Assert(_grid != null);

            if (args == null || args.Length == 0)
                return;

            if (args.Length == 3 && args[0] == null)
            {
                // Testing arguments passing.
                var key = args[1];
                var val = args[2];

                act(key, val);

                return;
            }

            if (LoadMultithreaded)
            {
                int cnt = 0;

                TestUtils.RunMultiThreaded(() => {
                    int i;

                    while ((i = Interlocked.Increment(ref cnt) - 1) < 1000)
                        act(i, "val_" + i);
                }, 8);
            }
            else
            {
                int start = (int)args[0];
                int cnt = (int)args[1];

                for (int i = start; i < start + cnt; i++)
                {
                    if (LoadObjects)
                        act(new Key(i), new Value(i));
                    else
                        act(i, "val_" + i);
                }
            }
        }

        public object Load(object key)
        {
            ThrowIfNeeded();

            Debug.Assert(_grid != null);

            return Map[key];
        }

        public IEnumerable<KeyValuePair<object, object>> LoadAll(IEnumerable<object> keys)
        {
            ThrowIfNeeded();

            Debug.Assert(_grid != null);

            return keys.ToDictionary(key => key, key =>(object)( "val_" + key));
        }

        public void Write(object key, object val)
        {
            ThrowIfNeeded();

            Debug.Assert(_grid != null);

            Map[key] = val;
        }

        public void WriteAll(IEnumerable<KeyValuePair<object, object>> map)
        {
            ThrowIfNeeded();

            Debug.Assert(_grid != null);

            foreach (var e in map)
                Map[e.Key] = e.Value;
        }

        public void Delete(object key)
        {
            ThrowIfNeeded();

            Debug.Assert(_grid != null);

            Map.Remove(key);
        }

        public void DeleteAll(IEnumerable<object> keys)
        {
            ThrowIfNeeded();

            Debug.Assert(_grid != null);

            foreach (object key in keys)
                Map.Remove(key);
        }

        public void SessionEnd(bool commit)
        {
            Debug.Assert(_grid != null);

            Debug.Assert(_ses != null);
        }

        public int IntProperty
        {
            get { return intProperty; }
            set { intProperty = value; }
        }

        public string StringProperty
        {
            get { return stringProperty; }
            set { stringProperty = value; }
        }

        private static void ThrowIfNeeded()
        {
            if (ThrowError)
                throw new CustomStoreException("Exception in cache store");
        }

        [Serializable]
        public class CustomStoreException : Exception, ISerializable
        {
            public string Details { get; private set; }

            public CustomStoreException(string message) : base(message)
            {
                Details = message;
            }

            protected CustomStoreException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
            {
                Details = info.GetString("details");
            }

            public override void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("details", Details);

                base.GetObjectData(info, context);
            }
        }
    }
}
