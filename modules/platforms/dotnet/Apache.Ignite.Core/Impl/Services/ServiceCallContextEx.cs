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
    using System.Collections;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service call context implementation.
    /// </summary>
    internal class ServiceCallContextEx : ServiceInterceptorContext
    {
        /** Context attributes. */
        private readonly IDictionary _attrs;

        /** Method name. */
        private readonly string _mtdName;

        /** Method arguments. */
        private readonly object[] _mtdArgs;

        /// <summary>
        /// Constructs context from dictionary.
        /// </summary>
        /// <param name="attrs">Context attributes.</param>
        /// <param name="mtdName">Method name.</param>
        /// <param name="mtdArgs">Method arguments.</param>
        internal ServiceCallContextEx(IDictionary attrs, string mtdName, object[] mtdArgs)
        {
            IgniteArgumentCheck.NotNull(attrs, "attrs");

            _attrs = attrs;
            _mtdName = mtdName;
            _mtdArgs = mtdArgs;
        }

        /** <inheritDoc /> */
        public override string GetAttribute(string name)
        {
            return (string) _attrs[name];
        }

        /** <inheritDoc /> */
        public override byte[] GetBinaryAttribute(string name)
        {
            return (byte[]) _attrs[name];
        }

        /** <inheritDoc /> */
        public override string GetMethod()
        {
            return _mtdName;
        }

        /** <inheritDoc /> */
        public override object[] GetArguments()
        {
            return _mtdArgs;
        }

        /** <inheritDoc /> */
        public override void SetAttribute(string name, string val)
        {
            _attrs[name] = val;
        }
        
        /** <inheritDoc /> */
        public override void SetBinaryAttribute(string name, byte[] val)
        {
            _attrs[name] = val;
        }

        /// <summary>
        /// Gets call context attributes.
        /// </summary>
        /// <returns>Service call context attributes.</returns>
        internal IDictionary Values()
        {
            return _attrs;
        }
    }
}