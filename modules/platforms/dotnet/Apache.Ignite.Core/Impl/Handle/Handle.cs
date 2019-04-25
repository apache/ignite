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

namespace Apache.Ignite.Core.Impl.Handle
{
    using System;
    using System.Threading;

    /// <summary>
    /// Wrapper over some resource ensuring it's release.
    /// </summary>
    internal class Handle<T> : IHandle
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
        protected Handle(T target, Action<T> releaseAction)
        {
            _target = target;
            _releaseAction = releaseAction;
        }

        /// <summary>
        /// Target.
        /// </summary>
        protected T Target
        {
            get { return _target; }
        }

        /// <summary>
        /// Release the resource.
        /// </summary>
        public void Release()
        {
            if (Interlocked.CompareExchange(ref _released, 1, 0) == 0)
                _releaseAction(_target);
        }
    }
}
