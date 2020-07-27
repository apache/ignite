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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// 
    /// </summary>
    public class LocalTransaction : ITransaction
    {
        /** <inheritDoc /> */
        public void Dispose()
        {
        }

        public Guid NodeId { get; private set; }
        public long ThreadId { get; private set; }
        public DateTime StartTime { get; private set; }
        public TransactionIsolation Isolation { get; private set; }
        public TransactionConcurrency Concurrency { get; private set; }
        public TransactionState State { get; private set; }
        public TimeSpan Timeout { get; private set; }
        public string Label { get; private set; }
        public bool IsRollbackOnly { get; private set; }

        /** <inheritDoc /> */
        public bool SetRollbackonly()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void Commit()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task CommitAsync()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void Rollback()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public Task RollbackAsync()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void AddMeta<TV>(string name, TV val)
        {
            throw new NotImplementedException();
        }

        public TV Meta<TV>(string name)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public TV RemoveMeta<TV>(string name)
        {
            throw new NotImplementedException();
        }
    }
}