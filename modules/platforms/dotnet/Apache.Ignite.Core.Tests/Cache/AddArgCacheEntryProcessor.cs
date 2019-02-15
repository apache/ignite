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

        /** <inheritdoc /> */
        public int Process(IMutableCacheEntry<int, int> entry, int arg)
        {
            throw new Exception("Invalid method");
        }
    }
}