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
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Composite service call interceptor.
    /// </summary>
    /// <seealso cref="IServiceCallInterceptor"/>
    internal class CompositeServiceCallInterceptor : IServiceCallInterceptor
    {
        // Service call interceptors.
        private readonly ICollection<IServiceCallInterceptor> _interceptors;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="interceptors">Service call interceptors.</param>
        public CompositeServiceCallInterceptor(IEnumerable<IServiceCallInterceptor> interceptors)
        {
            _interceptors = interceptors;
        }

        /** <inheritdoc /> */
        public object Invoke(string mtd, object[] args, IServiceContext ctx, Func<object> next)
        {
            return new CompositeCall(_interceptors, mtd, args, ctx, next).Invoke();
        }
        
        /// <summary>
        /// Composite call.
        /// </summary>
        private class CompositeCall
        {
            /** Interceptors enumerator. */
            private IEnumerator<IServiceCallInterceptor> _intcpsIter;

            /** Method name. */
            private readonly string _mtd;
            
            /** Method arguments. */
            private readonly object[] _args;
            
            /** Service context. */
            private readonly IServiceContext _ctx;
            
            /** Delegated call to a service method. */
            private readonly Func<object> _svcCall;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="interceptors">Service call interceptors.</param>
            /// <param name="mtd">Method name.</param>
            /// <param name="args">Method arguments.</param>
            /// <param name="ctx">Service context.</param>
            /// <param name="svcCall">Delegated call to a service method.</param>
            public CompositeCall(
                IEnumerable<IServiceCallInterceptor> interceptors,
                string mtd,
                object[] args,
                IServiceContext ctx,
                Func<object> svcCall
            ) {
                _mtd = mtd;
                _args = args;
                _ctx = ctx;
                _svcCall = svcCall;
                _intcpsIter = interceptors.GetEnumerator();

                IgniteArgumentCheck.Ensure(_intcpsIter.MoveNext(), "interceptors",
                    "Collection of interceptors must not be empty.");
            }

            /// <summary>
            /// Recursively invokes interceptors.
            /// </summary>
            /// <returns>Invocation result.</returns>
            public object Invoke()
            {
                var interceptor = _intcpsIter.Current;
                
                Debug.Assert(interceptor != null);

                return interceptor.Invoke(_mtd, _args, _ctx, _intcpsIter.MoveNext() ? Invoke : _svcCall);
            } 
        }
    }
}
