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
    using Apache.Ignite.Core.Impl.Services;

    /// <summary>
    /// Service call context.
    /// todo extended doc with examples
    /// </summary>
    public abstract class ServiceCallContext
    {
        /// <summary>
        /// Factory method for creating an internal implementation.
        /// </summary>
        /// <returns></returns>
        public static ServiceCallContext Create() {
            return new ServiceCallContextImpl();
        }

        /// <summary>
        /// Gets the string attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <returns>String attribute value.</returns>
        public abstract string Attribute(string name);

        /// <summary>
        /// Gets the binary attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <returns>Binary attribute value.</returns>
        public abstract byte[] BinaryAttribute(string name);
        
        /// <summary>
        /// Put new string attribute.
        /// If the context previously contained a mapping for the key, the old value is replaced by the specified value.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="value">Attribute value.</param>
        /// <returns>This for chaining.</returns>
        public abstract ServiceCallContext Put(string name, string value);

        /// <summary>
        /// Put new string attribute.
        /// If the context previously contained a mapping for the key, the old value is replaced by the specified value.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="value">Attribute value.</param>
        /// <returns>This for chaining.</returns>
        public abstract ServiceCallContext Put(string name, byte[] value);
    }
}