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

namespace Apache.Ignite.Core.Impl.Common
{
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Platform listenable.
    /// </summary>
    internal class Listenable : PlatformTarget
    {
        /** */
        private const int OpCancel = 1;

        /// <summary>
        /// Initializes a new instance of the <see cref="Listenable"/> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        public Listenable(IUnmanagedTarget target, Marshaller marsh) : base(target, marsh)
        {
            // No-op.
        }

        /// <summary>
        /// Cancels the listenable.
        /// </summary>
        public void Cancel()
        {
            DoOutInOp(OpCancel);
        }
    }
}
