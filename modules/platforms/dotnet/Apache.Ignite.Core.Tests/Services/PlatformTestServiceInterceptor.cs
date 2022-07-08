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

namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using Apache.Ignite.Core.Services;

    /** Test service call interceptor. */
    [Serializable]
    public class PlatformTestServiceInterceptor : IServiceCallInterceptor
    {
        /** Name of the method to intercept. */
        private string _targetMethod;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="targetMethod">Name of the method to intercept.</param>
        public PlatformTestServiceInterceptor(string targetMethod)
        {
            _targetMethod = targetMethod;
        }
        
        /** <inheritdoc /> */
        public object Invoke(string mtd, object[] args, IServiceContext ctx, Func<object> next)
        {
            object res = next.Invoke();

            if (_targetMethod.Equals(mtd))
                return (int)res * (int)res;

            return res;
        }
    }
}
