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

namespace Apache.Ignite.Core.Impl
{
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Lifecycle;

    /// <summary>
    /// Lifecycle bean holder.
    /// </summary>
    internal class LifecycleBeanHolder : ILifecycleBean
    {
        /** Target bean. */
        private readonly ILifecycleBean target;

        /** Whether start event was invoked. */
        private volatile bool startEvt;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target bean.</param>
        public LifecycleBeanHolder(ILifecycleBean target)
        {
            this.target = target;
        }

        /** <inheritDoc /> */
        public void OnLifecycleEvent(LifecycleEventType evt)
        {
            if (evt == LifecycleEventType.AFTER_GRID_START)
                // This event cannot be propagated right away because at this point we
                // do not have Ignite instance yet. So just schedule it.
                startEvt = true;
            else
                target.OnLifecycleEvent(evt);
        }

        /// <summary>
        /// Grid start callback.
        /// </summary>
        /// <param name="grid">Grid instance.</param>
        internal void OnStart(Ignite grid)
        {
            ResourceProcessor.Inject(target, grid);

            if (startEvt)
                target.OnLifecycleEvent(LifecycleEventType.AFTER_GRID_START);
        }
    }
}
