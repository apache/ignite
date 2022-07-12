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
    using System;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Represents service call interceptor.
    /// <para/>
    /// Allows the user to intercept the call to any service method except lifecycle methods
    /// (<see cref="IService.Init">Init</see>, <see cref="IService.Execute">Execute</see> and
    /// <see cref="IService.Cancel">Cancel</see>).
    /// <para/>
    /// A typical use of an interceptor is a middleware logic that applies to all custom methods in a service.
    /// <para/>
    /// The user can specify multiple interceptors in the <see cref="ServiceConfiguration">service configuration</see>.
    /// Each interceptor invokes the next interceptor in the chain using a delegated call, the last interceptor
    /// will call the service method.
    /// <example>
    /// <code>
    /// class Security : IServiceCallInterceptor
    /// {
    ///   public object Invoke(string mtd, object[] args, IServiceContext ctx, Func&lt;object&gt; next)
    ///   {
    ///     if (!CustomSecurityProvider.Instance().Access(ctx.CurrentCallContext.GetAttribute("sessionId")))
    ///       throw new SecurityException();
    ///
    ///     // Execute remaining interceptors and service method.
    ///     return next.Invoke();
    ///   }
    /// }
    ///
    /// class Audit : IServiceCallInterceptor
    /// {
    ///   public object Invoke(string mtd, object[] args, IServiceContext ctx, Func&lt;object&gt; next)
    ///   {
    ///     var sessionId = ctx.CurrentCallContext.GetAttribute("sessionId");
    ///     var audit = AuditProvider.Instance();
    ///
    ///     audit.RecordEvent("start", mtd, sessionId);
    ///
    ///     try
    ///     {
    ///       // Execute service method.
    ///       return next.Invoke();
    ///     }
    ///     catch (Exception e)
    ///     {
    ///       // Record error.
    ///       audit.RecordEvent("error", mtd, "id=" + sessionId + ", err=" + e.Message);
    ///
    ///       // Re-throw exception to initiator.
    ///       throw;
    ///     }
    ///     finally
    ///     {
    ///       // Record finish event after execution of the service method.
    ///       audit.RecordEvent("finish", mtd, sessionId);
    ///     }
    ///   }
    /// }
    /// 
    /// ...
    /// 
    /// var svcCfg = new ServiceConfiguration()
    /// {
    ///   Name = "service",
    ///   Service = new MyService(),
    ///   MaxPerNodeCount = 1,
    ///   Interceptors = new List&lt;IServiceCallInterceptor&gt; { new Audit(), new Security() }
    /// };
    ///
    /// // Deploy service.
    /// ignite.GetServices().Deploy(svcCfg);
    ///
    /// // Set context parameters for the service proxy.
    /// var callCtx = new ServiceCallContextBuilder().Set("sessionId", sessionId).Build();
    ///
    /// // Make service proxy with caller context to define sessionId attribute.
    /// var proxy = ignite.GetServices().GetServiceProxy&lt;IMyService&gt;("service", false, callCtx);
    ///
    /// // Service method call will be intercepted.
    /// proxy.PlaceOrder(order1);
    /// proxy.PlaceOrder(order2);
    /// </code>
    /// </example>
    /// </summary>
    /// <seealso cref="IServiceCallContext"/>
    /// <seealso cref="IServiceContext"/>
    [IgniteExperimental]
    public interface IServiceCallInterceptor
    {
        /// <summary>
        /// Intercepts delegated service call.
        /// </summary>
        /// <param name="mtd">Method name.</param>
        /// <param name="args">Method arguments.</param>
        /// <param name="ctx">Service context.</param>
        /// <param name="next">Delegated call to a service method and/or interceptor in the chain.</param>
        /// <returns>Service call result.</returns>
        public object Invoke(string mtd, object[] args, IServiceContext ctx, Func<object> next);
    }
}
