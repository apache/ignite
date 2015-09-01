/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
