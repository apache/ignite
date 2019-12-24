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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using Apache.Ignite.Core.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Cache entry processor that adds argument value to the entry value.
    /// </summary>
    [Serializable]
    public class AddArgCacheEntryProcessor : ICacheEntryProcessor<int, int, int, int>
    {
        // Expected exception text
        public const string ExceptionText = "Exception from AddArgCacheEntryProcessor.";

        // Error flag
        public bool ThrowErr { get; set; }

        // Error flag
        public bool ThrowErrBinarizable { get; set; }

        // Error flag
        public bool ThrowErrNonSerializable { get; set; }

        // Key value to throw error on
        public int ThrowOnKey { get; set; }

        // Remove flag
        public bool Remove { get; set; }

        // Exists flag
        public bool Exists { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AddArgCacheEntryProcessor"/> class.
        /// </summary>
        public AddArgCacheEntryProcessor()
        {
            Exists = true;
            ThrowOnKey = -1;
        }

        /** <inheritdoc /> */
        int ICacheEntryProcessor<int, int, int, int>.Process(IMutableCacheEntry<int, int> entry, int arg)
        {
            if (ThrowOnKey < 0 || ThrowOnKey == entry.Key)
            {
                if (ThrowErr)
                    throw new Exception(ExceptionText);

                if (ThrowErrBinarizable)
                    throw new BinarizableTestException {Info = ExceptionText};

                if (ThrowErrNonSerializable)
                    throw new NonSerializableException();
            }

            Assert.AreEqual(Exists, entry.Exists);

            if (Remove)
                entry.Remove();
            else
                entry.Value = entry.Value + arg;
            
            return entry.Value;
        }

        /// <summary>
        /// Invalid override.
        /// </summary>
        public int Process(IMutableCacheEntry<int, int> entry, int arg)
        {
            throw new Exception("Invalid method");
        }
    }
}