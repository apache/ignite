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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Tests.Client.Cache;

    /// <summary>
    /// Test service interface.
    /// </summary>
    public interface ITestService
    {
        /** */
        int IntProperty { get; set; }

        /** */
        Person PersonProperty { get; set; }

        /** */
        void VoidMethod();

        /** */
        int IntMethod();

        /** */
        void ExceptionalMethod();

        /** */
        Task<int> AsyncMethod();

        /** */
        Person PersonMethod(Person person);

        /** */
        IBinaryObject PersonMethodBinary(IBinaryObject person);

        /** */
        Person[] PersonArrayMethod(Person[] persons);

        /** */
        IBinaryObject[] PersonArrayMethodBinary(IBinaryObject[] persons);

        /** */
        void Sleep(TimeSpan delay);

        /** */
        Guid GetNodeId();
        
        /** */
        string ContextAttribute(string name);
        
        /** */
        byte[] ContextBinaryAttribute(string name);
    }
}
