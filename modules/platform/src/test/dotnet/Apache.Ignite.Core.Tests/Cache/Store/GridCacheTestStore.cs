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

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Resource;

    [SuppressMessage("ReSharper", "FieldCanBeMadeReadOnly.Local")]
    public class GridCacheTestStore : ICacheStore
    {
        public static readonly IDictionary MAP = new ConcurrentDictionary<object, object>();

        public static bool expCommit;
        
        public static bool loadMultithreaded;

        public static bool loadObjects;

        [InstanceResource]
        private IIgnite grid = null;

        [StoreSessionResource]
#pragma warning disable 649
        private ICacheStoreSession ses;
#pragma warning restore 649

        public static int intProperty;

        public static string stringProperty;

        public static void Reset()
        {
            MAP.Clear();

            expCommit = false;
            loadMultithreaded = false;
            loadObjects = false;
        }

        public void LoadCache(Action<object, object> act, params object[] args)
        {
            Debug.Assert(grid != null);

            if (loadMultithreaded)
            {
                int cnt = 0;

                GridTestUtils.RunMultiThreaded(() => {
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
                    if (loadObjects)
                        act(new Key(i), new Value(i));
                    else
                        act(i, "val_" + i);
                }
            }
        }

        public object Load(object key)
        {
            Debug.Assert(grid != null);

            return MAP[key];
        }

        public IDictionary LoadAll(ICollection keys)
        {
            Debug.Assert(grid != null);

            return keys.OfType<object>().ToDictionary(key => key, Load);
        }

        public void Write(object key, object val)
        {
            Debug.Assert(grid != null);

            MAP[key] = val;
        }

        public void WriteAll(IDictionary map)
        {
            Debug.Assert(grid != null);

            foreach (DictionaryEntry e in map)
                MAP[e.Key] = e.Value;
        }

        public void Delete(object key)
        {
            Debug.Assert(grid != null);

            MAP.Remove(key);
        }

        public void DeleteAll(ICollection keys)
        {
            Debug.Assert(grid != null);

            foreach (object key in keys)
                MAP.Remove(key);
        }

        public void SessionEnd(bool commit)
        {
            Debug.Assert(grid != null);

            Debug.Assert(ses != null);
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
    }
}
