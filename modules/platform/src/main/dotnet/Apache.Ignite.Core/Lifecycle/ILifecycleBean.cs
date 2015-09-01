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
    using GridGain.Resource;

    /// <summary>
    /// A bean that reacts to grid lifecycle events defined in <see cref="LifecycleEventType"/>.
    /// Use this bean whenever you need to plug some custom logic before or after
    /// grid startup and stopping routines.
    /// <para />
    /// There are four events you can react to:
    /// <list type="bullet">
    ///     <item>
    ///         <term>BEFORE_GRID_START</term>
    ///         <description>Invoked before grid startup routine is initiated. Note that grid 
    ///         is not available during this event, therefore if you injected a grid instance 
    ///         via <see cref="InstanceResourceAttribute"/> attribute, you cannot 
    ///         use it yet.</description>
    ///     </item>
    ///     <item>
    ///         <term>AFTER_GRID_START</term>
    ///         <description>Invoked right after grid has started. At this point, if you injected
    ///         a grid instance via <see cref="InstanceResourceAttribute"/> attribute, 
    ///         you can start using it.</description>
    ///     </item>
    ///     <item>
    ///         <term>BEFORE_GRID_STOP</term>
    ///         <description>Invoked right before grid stop routine is initiated. Grid is still 
    ///         available at this stage, so if you injected a grid instance via 
    ///         <see cref="InstanceResourceAttribute"/> attribute, you can use it.
    ///         </description>
    ///     </item>
    ///     <item>
    ///         <term>AFTER_GRID_STOP</term>
    ///         <description>Invoked right after grid has stopped. Note that grid is not available 
    ///         during this event.</description>
    ///     </item>
    /// </list>
    /// </summary>
    public interface ILifecycleBean
    {
        /// <summary>
        /// This method is called when lifecycle event occurs.
        /// </summary>
        /// <param name="evt">Lifecycle event.</param>
        void OnLifecycleEvent(LifecycleEventType evt);
    }
}
