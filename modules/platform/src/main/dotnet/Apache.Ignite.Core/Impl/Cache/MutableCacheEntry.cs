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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Represents a cache entry.
    /// </summary>
    internal class MutableCacheEntry<TK, TV> : IMutableCacheEntry<TK, TV>, IMutableCacheEntryInternal
    {
        // Entry value
        private TV _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="MutableCacheEntry{K, V}"/> class.
        /// </summary>
        /// <param name="key">The key.</param>
        public MutableCacheEntry(TK key)
        {
            Key = key;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MutableCacheEntry{K, V}"/> class.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public MutableCacheEntry(TK key, TV value)
        {
            Key = key;
            _value = value;
            Exists = true;
        }

        /** <inheritdoc /> */
        public TK Key { get; private set; }

        /** <inheritdoc /> */
        object IMutableCacheEntryInternal.Key
        {
            get { return Key; }
        }

        /** <inheritdoc /> */
        public TV Value
        {
            get { return _value; }
            set
            {
                _value = value;
                Exists = true;
                State = MutableCacheEntryState.ValueSet;
            }
        }

        /** <inheritdoc /> */
        object IMutableCacheEntryInternal.Value
        {
            get { return Value; }
        }

        /** <inheritdoc /> */
        public bool Exists { get; private set; }

        /** <inheritdoc /> */
        public void Remove()
        {
            Value = default(TV);
            Exists = false;
            State = MutableCacheEntryState.Removed;
        }

        /** <inheritdoc /> */
        public MutableCacheEntryState State { get; private set; }
    }

    /// <summary>
    /// Internal non-generic representation of a mutable cache entry.
    /// </summary>
    internal interface IMutableCacheEntryInternal
    {
        /// <summary>
        /// Gets the key.
        /// </summary>
        object Key { get; }

        /// <summary>
        /// Gets the value.
        /// </summary>
        object Value { get; }

        /// <summary>
        /// Gets a value indicating whether cache entry exists.
        /// </summary>
        bool Exists { get; }

        /// <summary>
        /// Gets the state indicating user operation on this instance.
        /// </summary>
        MutableCacheEntryState State { get; }
    }

    /// <summary>
    /// Mutable cache entry factory.
    /// </summary>
    internal static class MutableCacheEntry
    {
        private static readonly CopyOnWriteConcurrentDictionary<Tuple<Type, Type>, Func<object, object, bool, IMutableCacheEntryInternal>> 
            Ctors = new CopyOnWriteConcurrentDictionary<Tuple<Type, Type>, Func<object, object, bool, IMutableCacheEntryInternal>>();

        public static Func<object, object, bool, IMutableCacheEntryInternal> GetCtor(Type keyType, Type valType)
        {
            Func<object, object, bool, IMutableCacheEntryInternal> result;
            var funcKey = new Tuple<Type, Type>(keyType, valType);

            return Ctors.TryGetValue(funcKey, out result)
                ? result
                : Ctors.GetOrAdd(funcKey, x =>
                {
                    var entryType = typeof (MutableCacheEntry<,>).MakeGenericType(keyType, valType);

                    var oneArg = DelegateConverter.CompileCtor<Func<object, IMutableCacheEntryInternal>>(entryType,
                        new[] {keyType}, false);

                    var twoArg =
                        DelegateConverter.CompileCtor<Func<object, object, IMutableCacheEntryInternal>>(entryType, 
                        new[] {keyType, valType}, false);

                    return (k, v, exists) => exists ? twoArg(k, v) : oneArg(k);
                });
        }
    }

    /// <summary>
    /// Represents result of user operation on a mutable cache entry.
    /// </summary>
    internal enum MutableCacheEntryState : byte
    {
        Intact = 0,
        ValueSet = 1,
        Removed = 2,
        ErrPortable = 3,
        ErrString = 4
    }
}