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
    /// <summary>
    /// Represents Ignite-managed service.
    /// </summary>
    public interface IService
    {
        /// <summary>
        /// Initializes this instance before execution.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        void Init(IServiceContext context);

        /// <summary>
        /// Starts execution of this service. This method is automatically invoked whenever an instance of the service
        /// is deployed on a Ignite node. Note that service is considered deployed even after it exits the Execute
        /// method and can be cancelled (or undeployed) only by calling any of the Cancel methods on 
        /// <see cref="IServices"/> API. Also note that service is not required to exit from Execute method until
        /// Cancel method was called.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        void Execute(IServiceContext context);

        /// <summary>
        /// Cancels this instance.
        /// <para/>
        /// Note that Ignite cannot guarantee that the service exits from <see cref="IService.Execute"/>
        /// method whenever <see cref="IService.Cancel"/> is called. It is up to the user to
        /// make sure that the service code properly reacts to cancellations.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        void Cancel(IServiceContext context);
    }
}