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
    /// <summary>
    /// Ignite lifecycle event types. These events are used to notify lifecycle beans
    /// about changes in Ignite lifecycle state.
    /// <para />
    /// For more information and detailed examples refer to <see cref="ILifecycleBean"/>
    /// documentation.
    /// </summary>
    public enum LifecycleEventType
    {
        /// <summary>
        /// Invoked before node startup routine. Node is not initialized and cannot be used.
        /// </summary>
        BeforeNodeStart,

        /// <summary>
        /// Invoked after node startup is complete. Node is fully initialized and fully functional.
        /// </summary>
        AfterNodeStart,

        /// <summary>
        /// Invoked before node stopping routine. Node is fully functional at this point.
        /// </summary>
        BeforeNodeStop,

        /// <summary>
        /// Invoked after node had stopped. Node is stopped and cannot be used. 
        /// </summary>
        AfterNodeStop
    }
}
