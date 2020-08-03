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

namespace Apache.Ignite.Core.Impl.Transactions
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Internal transaction read only disposable collection.
    /// </summary>
    internal sealed class TransactionCollectionImpl : ITransactionCollection
    {
        /** */
        private readonly ICollection<ITransaction> _col;
        
        ///<summary>
        /// Initialize <see cref="TransactionCollectionImpl"/> by wrapping.
        /// </summary> 
        public TransactionCollectionImpl(ICollection<ITransaction> col)
        {
            _col = col;
        }

        /** <inheritdoc /> */
        public IEnumerator<ITransaction> GetEnumerator()
        {
            return _col.GetEnumerator();
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable) _col).GetEnumerator();
        }

        /** <inheritdoc /> */
        public void Add(ITransaction item)
        {
            throw GetReadOnlyException();
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            throw GetReadOnlyException();
        }

        /** <inheritdoc /> */
        public bool Contains(ITransaction item)
        {
            return _col.Contains(item);
        }

        /** <inheritdoc /> */
        public void CopyTo(ITransaction[] array, int arrayIndex)
        {
            _col.CopyTo(array, arrayIndex);
        }

        /** <inheritdoc /> */
        public bool Remove(ITransaction item)
        {
            throw GetReadOnlyException();
        }

        /** <inheritdoc /> */
        public int Count
        {
            get { return _col.Count; }
        }

        /** <inheritdoc /> */
        public bool IsReadOnly
        {
            get { return true; }
        }

        /// <summary>
        /// Gets the readonly exception.
        /// </summary>
        private static Exception GetReadOnlyException()
        {
            return new NotSupportedException("Collection is read-only.");
        }
        
        /** <inheritdoc /> */
        public void Dispose()
        {  
            foreach (var tx in _col)
            {
                tx.Dispose();
            }
        }
    }
}
