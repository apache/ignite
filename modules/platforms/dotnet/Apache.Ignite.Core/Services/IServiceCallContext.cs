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

namespace Apache.Ignite.Core.Services
{
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Represents service call context.
    /// <para />
    /// This context is implicitly passed to the service and can be retrieved inside the service
    /// using <see cref="IServiceContext.CurrentCallContext()"/>. It is accessible only
    /// from the local thread during the execution of a service method.
    /// <para />
    /// Use <see cref="ServiceCallContextBuilder"/> to instantiate the context.
    /// <para />
    /// <b>Note</b>: passing the context to the service may lead to performance overhead,
    /// so it should only be used for "middleware" tasks.
    /// <para />
    /// Usage example:
    /// <code>
    /// // Service implementation.
    /// public class HelloServiceImpl : HelloService
    /// {
    ///     private IServiceContext ctx;
    ///
    ///     public void Init(IServiceContext ctx)
    ///     {
    ///         this.ctx = ctx;
    ///     }
    /// 
    ///     public string Call(string msg)
    ///     {
    ///         return msg + ctx.CurrentCallContext.Attribute("user");
    ///     }
    ///     ...
    /// }
    /// ...
    ///
    /// // Call this service with context.
    /// IServiceCallContext callCtx = new ServiceCallContextBuilder().Set("user", "John").build();
    /// HelloService helloSvc = ignite.GetServices().GetServiceProxy&lt;HelloService&gt;("hello-service", false, callCtx);
    /// // Print "Hello John".
    /// Console.WriteLine( helloSvc.call("Hello ") );
    /// </code>
    /// </summary>
    /// <seealso cref="IServiceCallInterceptor"/>
    /// <seealso cref="IServiceContext"/>
    /// <seealso cref="ServiceCallContextBuilder"/>
    [IgniteExperimental]
    public interface IServiceCallContext
    {
        /// <summary>
        /// Gets the string attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <returns>String attribute value.</returns>
        string GetAttribute(string name);

        /// <summary>
        /// Gets the binary attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <returns>Binary attribute value.</returns>
        byte[] GetBinaryAttribute(string name);
    }
}