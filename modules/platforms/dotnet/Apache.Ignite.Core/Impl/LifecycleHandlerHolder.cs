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

namespace Apache.Ignite.Core.Impl
{
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Lifecycle;

    /// <summary>
    /// Lifecycle bean holder.
    /// </summary>
    internal class LifecycleHandlerHolder
    {
        /** Target bean. */
        private readonly ILifecycleHandler _target;

        /** Whether start event was invoked. */
        private volatile bool _startEvt;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target bean.</param>
        public LifecycleHandlerHolder(ILifecycleHandler target)
        {
            _target = target;
        }

        /** <inheritDoc /> */
        public void OnLifecycleEvent(LifecycleEventType evt)
        {
            if (evt == LifecycleEventType.AfterNodeStart)
                // This event cannot be propagated right away because at this point we
                // do not have Ignite instance yet. So just schedule it.
                _startEvt = true;
            else
                _target.OnLifecycleEvent(evt);
        }

        /// <summary>
        /// Grid start callback.
        /// </summary>
        /// <param name="grid">Ignite instance.</param>
        internal void OnStart(Ignite grid)
        {
            ResourceProcessor.Inject(_target, grid);

            if (_startEvt)
                _target.OnLifecycleEvent(LifecycleEventType.AfterNodeStart);
        }
    }
}
