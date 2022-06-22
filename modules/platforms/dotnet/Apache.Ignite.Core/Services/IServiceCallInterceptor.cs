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

    /// <summary>
    /// Represents Ignite service method invocation interceptor.
    /// </summary>
    [Serializable]
    public abstract class ServiceCallInterceptor
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ctx"></param>
        public virtual void OnInvoke(ServiceInterceptorContext ctx)
        {
            // No-op by default.
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="res"></param>
        /// <param name="ctx"></param>
        public virtual void OnComplete(object res, ServiceInterceptorContext ctx)
        {
            // No-op by default.
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="err"></param>
        /// <param name="ctx"></param>
        public virtual void OnError(Exception err, ServiceInterceptorContext ctx)
        {
            // No-op by default.
        }
    }
}