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

namespace Apache.Ignite.Core.Impl.Handle
{
    using System;
    using System.Threading;

    /// <summary>
    /// Wrapper over some resource ensuring it's release.
    /// </summary>
    public class Handle<T> : IHandle
    {
        /** Target.*/
        private readonly T _target;

        /** Release action. */
        private readonly Action<T> _releaseAction; 

        /** Release flag. */
        private int _released;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="releaseAction">Release action.</param>
        public Handle(T target, Action<T> releaseAction)
        {
            _target = target;
            _releaseAction = releaseAction;
        }

        /// <summary>
        /// Target.
        /// </summary>
        public T Target
        {
            get { return _target; }
        }

        /** <inheritdoc /> */
        public void Release()
        {
            if (Interlocked.CompareExchange(ref _released, 1, 0) == 0)
                _releaseAction(_target);
        }

        /** <inheritdoc /> */
        public bool Released
        {
            get { return Thread.VolatileRead(ref _released) == 1; }
        }
    }
}
