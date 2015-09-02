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

namespace Apache.Ignite.Core.Lifecycle
{
    using Apache.Ignite.Core.Resource;

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
