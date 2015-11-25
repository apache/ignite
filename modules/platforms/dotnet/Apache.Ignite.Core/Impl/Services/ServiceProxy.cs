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
    using System.Reflection;
    using System.Runtime.Remoting.Messaging;
    using System.Runtime.Remoting.Proxies;

    /// <summary>
    /// Service proxy: user works with a remote service as if it is a local object.
    /// </summary>
    /// <typeparam name="T">User type to be proxied.</typeparam>
    internal class ServiceProxy<T> : RealProxy
    {
        /** Services. */
        private readonly Func<MethodBase, object[], object> _invokeAction;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceProxy{T}" /> class.
        /// </summary>
        /// <param name="invokeAction">Method invoke action.</param>
        public ServiceProxy(Func<MethodBase, object[], object> invokeAction)
            : base(typeof (T))
        {
            Debug.Assert(invokeAction != null);

            _invokeAction = invokeAction;
        }

        /** <inheritdoc /> */
        public override IMessage Invoke(IMessage msg)
        {
            var methodCall = msg as IMethodCallMessage;

            if (methodCall == null)
                throw new NotSupportedException("Service proxy operation type not supported: " + msg.GetType() +
                                                ". Only method and property calls are supported.");

            if (methodCall.InArgCount != methodCall.ArgCount)
                throw new NotSupportedException("Service proxy does not support out arguments: "
                                                + methodCall.MethodBase);

            var result = _invokeAction(methodCall.MethodBase, methodCall.Args);

            return new ReturnMessage(result, null, 0, methodCall.LogicalCallContext, methodCall);
        }

        /** <inheritdoc /> */
        public new T GetTransparentProxy()
        {
            return (T) base.GetTransparentProxy();
        }
    }
}