/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    using System.Linq.Expressions;
    using System.Reflection;
    using ProxyAction = System.Func<System.Reflection.MethodBase, object[], object>;

    /// <summary>
    /// Factory for proxy creation.
    /// </summary>
    /// <typeparam name="T">User type to be proxied.</typeparam>
    internal static class ServiceProxyFactory<T>
    {
        /** */
        private static readonly Func<ProxyAction, T> Factory = GenerateFactory();

        /// <summary>
        /// Creates proxy which methods call provided function.
        /// </summary>
        /// <param name="action">Action to call.</param>
        /// <returns>Proxy.</returns>
        public static T CreateProxy(ProxyAction action)
        {
            Debug.Assert(action != null);

            return Factory(action);
        }

        /// <summary>
        /// Generates the proxy factory.
        /// </summary>
        private static Func<ProxyAction, T> GenerateFactory()
        {
            // Generate proxy class
            var result = ServiceProxyTypeGenerator.Generate(typeof(T));
            var typeCtr = result.Item1.GetConstructor(new[] { typeof(ProxyAction), typeof(MethodInfo[]) });
            Debug.Assert(typeCtr != null);

            // Generate method that creates proxy class instance.
            // Single parameter of method
            var action = Expression.Parameter(typeof(ProxyAction));

            // Call constructor and pass action parameter and array of methods.
            var ctr = Expression.New(typeCtr, action, Expression.Constant(result.Item2));
            var lambda = Expression.Lambda<Func<ProxyAction, T>>(ctr, action);

            return lambda.Compile();
        }
    }
}