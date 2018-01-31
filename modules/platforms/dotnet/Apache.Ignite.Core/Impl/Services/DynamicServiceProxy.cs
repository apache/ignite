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

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Diagnostics;
    using System.Dynamic;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service proxy based on DynamicObject to be used with <c>dynamic</c> keyword
    /// and <see cref="IServices.GetDynamicServiceProxy"/>.
    /// </summary>
    internal class DynamicServiceProxy : DynamicObject
    {
        private readonly Func<string, object[], object> _invokeMethod;

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicServiceProxy"/> class.
        /// </summary>
        /// <param name="invokeMethod">The service invoke method.</param>
        public DynamicServiceProxy(Func<string, object[], object> invokeMethod)
        {
            Debug.Assert(invokeMethod != null);

            _invokeMethod = invokeMethod;
        }


        /// <summary>
        /// Provides the implementation for operations that get member values.
        /// Classes derived from the <see cref="DynamicObject" /> class can override this method
        /// to specify dynamic behavior for operations such as getting a value for a property.
        /// </summary>
        /// <param name="binder">Provides information about the object that called the dynamic operation.
        /// The binder.Name property provides the name of the member on which the dynamic operation is performed.
        /// </param>
        /// <param name="result">The result of the get operation.</param>
        /// <returns>
        /// true if the operation is successful; otherwise, false. If this method returns false,
        /// the run-time binder of the language determines the behavior.
        /// (In most cases, a run-time exception is thrown.)
        /// </returns>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = _invokeMethod(binder.Name, null);
            return true;
        }
    }
}
