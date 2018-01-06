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
    using System.Linq.Expressions;
    using ProxyAction = System.Func<System.Reflection.MethodBase, object[], object>;

    /// <summary>
    /// Factory for proxy creation.
    /// </summary>
    /// <typeparam name="T">User type to be proxied.</typeparam>
    internal static class ServiceProxyFactory<T>
    {
        private static readonly Func<ProxyAction, T> _factory = GenerateFactory();

        /// <summary>
        /// Creates proxy which methods call provided function.
        /// </summary>
        /// <param name="action">Action to call.</param>
        /// <returns>Proxy.</returns>
        public static T CreateProxy(ProxyAction action)
        {
            return _factory(action);
        }

        private static Func<ProxyAction, T> GenerateFactory()
        {
            //generate proxy class
            var proxyType = ServiceProxyTypeGenerator.Generate(typeof(T));

            //generate method that creates proxy class instance.
            //single parameters
            var action = Expression.Parameter(typeof(ProxyAction));
            var ctr = Expression.New(proxyType.GetConstructor(new[] {action.Type}), action);
            var lambda = Expression.Lambda<Func<ProxyAction, T>>(ctr, action);
            return lambda.Compile();
        }
    }
}