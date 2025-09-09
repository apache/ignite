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

namespace Apache.Ignite.Core.Tests.Client.Services
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Tests.Client.Cache;

    /// <summary>
    /// Test service.
    /// </summary>
    public class TestService : ITestService, IService
    {
        /** */
        [InstanceResource]
        private readonly IIgnite _ignite = null;
        
        /** Service context. */
        private IServiceContext _ctx;

        /** */
        public const string ExceptionText = "Some error";

        /** */
        public static int CallCount { get; set; }

        /** <inheritdoc /> */
        public int IntProperty { get; set; }

        /** <inheritdoc /> */
        public Person PersonProperty { get; set; }

        /** <inheritdoc /> */
        public void VoidMethod()
        {
            CallCount++;
        }

        /** <inheritdoc /> */
        public int IntMethod()
        {
            return 42;
        }

        /** <inheritdoc /> */
        public void ExceptionalMethod()
        {
            throw new ArithmeticException(ExceptionText);
        }

        /** <inheritdoc /> */
        public Task<int> AsyncMethod()
        {
            var tcs = new TaskCompletionSource<int>();
            new Timer(_ => tcs.SetResult(1)).Change(500, -1);
            return tcs.Task;
        }

        /** <inheritdoc /> */
        public Person PersonMethod(Person person)
        {
            return new Person(person.Id + 1);
        }

        /** <inheritdoc /> */
        public IBinaryObject PersonMethodBinary(IBinaryObject person)
        {
            return person
                .ToBuilder()
                .SetField("Id", person.GetField<int>("Id") + 1)
                .Build();
        }

        /** <inheritdoc /> */
        public Person[] PersonArrayMethod(Person[] persons)
        {
            return persons.Select(p => new Person(p.Id + 2)).ToArray();
        }

        /** <inheritdoc /> */
        public IBinaryObject[] PersonArrayMethodBinary(IBinaryObject[] persons)
        {
            return persons
                .Select(p => p.ToBuilder().SetIntField("Id", p.GetField<int>("Id") + 2).Build())
                .ToArray();
        }

        /** <inheritdoc /> */
        public void Sleep(TimeSpan delay)
        {
            Thread.Sleep(delay);
        }

        /** <inheritdoc /> */
        public Guid GetNodeId()
        {
            return _ignite.GetCluster().GetLocalNode().Id;
        }
        
        /** <inheritdoc /> */
        public string ContextAttribute(string name)
        {
            IServiceCallContext callCtx = _ctx.CurrentCallContext;

            return callCtx == null ? null : callCtx.GetAttribute(name);
        }
        
        /** <inheritdoc /> */
        public byte[] ContextBinaryAttribute(string name)
        {
            IServiceCallContext callCtx = _ctx.CurrentCallContext;

            return callCtx == null ? null : callCtx.GetBinaryAttribute(name);
        }

        /** <inheritdoc /> */
        public void Init(IServiceContext context)
        {
            _ctx = context;
        }

        /** <inheritdoc /> */
        public void Execute(IServiceContext context)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void Cancel(IServiceContext context)
        {
            // No-op.
        }
    }
}
