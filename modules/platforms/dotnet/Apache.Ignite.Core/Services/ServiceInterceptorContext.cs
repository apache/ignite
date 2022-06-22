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
    /// Represents mutable service call context.
    /// </summary>
    [IgniteExperimental]
    public abstract class ServiceInterceptorContext : IServiceCallContext
    {
        /** <inheritDoc /> */
        public abstract string GetAttribute(string name);

        /** <inheritDoc /> */
        public abstract byte[] GetBinaryAttribute(string name);

        /// <summary>
        /// Sets the string attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="val">Attribute value.</param>
        public abstract void SetAttribute(string name, string val);
        
        /// <summary>
        /// Sets the binary attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="val">Binary attribute value.</param>
        public abstract void SetBinaryAttribute(string name, byte[] val);

        /// <summary>
        /// Gets invocation method name.
        /// </summary>
        /// <returns>Method name.</returns>
        public abstract string GetMethod();

        /// <summary>
        /// Gets method arguments.
        /// </summary>
        /// <returns>Method arguments.</returns>
        public abstract object[] GetArguments();
    }
}