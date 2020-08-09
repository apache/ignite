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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Tests.Client.Cache;

    public class TestService : ITestService, IService
    {
        public const string ExceptionText = "Some error";
        
        public static int CallCount { get; set; }

        public void VoidMethod()
        {
            CallCount++;
        }

        public int IntMethod()
        {
            return 42;
        }

        public void ExceptionalMethod()
        {
            throw new ArithmeticException(ExceptionText);
        }

        public Task<int> AsyncMethod()
        {
            return Task.Delay(500).ContinueWith(_ => 1);
        }

        public Person PersonMethod(Person person)
        {
            return new Person(person.Id + 1);
        }

        public void Init(IServiceContext context)
        {
            // No-op.
        }

        public void Execute(IServiceContext context)
        {
            // No-op.
        }

        public void Cancel(IServiceContext context)
        {
            // No-op.
        }
    }
}
