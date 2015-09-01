/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */


namespace GridGain.Lifecycle
{
    /// <summary>
    /// Grid lifecycle event types. These events are used to notify lifecycle beans
    /// about changes in grid lifecycle state.
    /// <para />
    /// For more information and detailed examples refer to <see cref="ILifecycleBean"/>
    /// documentation.
    /// </summary>
    public enum LifecycleEventType
    {
        /// <summary>
        /// Invoked before grid startup routine. Grid is not initialized and cannot be used.
        /// </summary>
        BEFORE_GRID_START,

        /// <summary>
        /// Invoked after grid startup is complete. Grid is fully initialized and fully functional.
        /// </summary>
        AFTER_GRID_START,

        /// <summary>
        /// Invoked before grid stopping routine. Grid is fully functional at this point.
        /// </summary>
        BEFORE_GRID_STOP,

        /// <summary>
        /// Invoked after grid had stopped. Grid is stopped and cannot be used. 
        /// </summary>
        AFTER_GRID_STOP
    }
}
