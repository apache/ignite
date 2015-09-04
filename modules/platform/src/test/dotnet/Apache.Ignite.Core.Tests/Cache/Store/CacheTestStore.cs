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
    public class CacheTestStore : ICacheStore
    {
        public static readonly IDictionary Map = new ConcurrentDictionary<object, object>();

        public static bool ExpCommit;
        
        public static bool LoadMultithreaded;

        public static bool LoadObjects;

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

            ExpCommit = false;
            LoadMultithreaded = false;
            LoadObjects = false;
        }

        public void LoadCache(Action<object, object> act, params object[] args)
        {
            Debug.Assert(_grid != null);

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
            Debug.Assert(_grid != null);

            return Map[key];
        }

        public IDictionary LoadAll(ICollection keys)
        {
            Debug.Assert(_grid != null);

            return keys.OfType<object>().ToDictionary(key => key, Load);
        }

        public void Write(object key, object val)
        {
            Debug.Assert(_grid != null);

            Map[key] = val;
        }

        public void WriteAll(IDictionary map)
        {
            Debug.Assert(_grid != null);

            foreach (DictionaryEntry e in map)
                Map[e.Key] = e.Value;
        }

        public void Delete(object key)
        {
            Debug.Assert(_grid != null);

            Map.Remove(key);
        }

        public void DeleteAll(ICollection keys)
        {
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
    }
}
