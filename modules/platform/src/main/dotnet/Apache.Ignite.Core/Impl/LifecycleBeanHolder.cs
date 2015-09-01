/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl
{
    using GridGain.Impl.Resource;
    using GridGain.Lifecycle;

    using U = GridGain.Impl.GridUtils;

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
                // do not have GridImpl instance yet. So just schedule it.
                startEvt = true;
            else
                target.OnLifecycleEvent(evt);
        }

        /// <summary>
        /// Grid start callback.
        /// </summary>
        /// <param name="grid">Grid instance.</param>
        internal void OnStart(GridImpl grid)
        {
            ResourceProcessor.Inject(target, grid);

            if (startEvt)
                target.OnLifecycleEvent(LifecycleEventType.AFTER_GRID_START);
        }
    }
}
