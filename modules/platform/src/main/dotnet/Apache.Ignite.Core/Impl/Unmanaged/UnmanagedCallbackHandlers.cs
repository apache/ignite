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

namespace Apache.Ignite.Core.Impl.Unmanaged
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// Unmanaged callback handler function pointers.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct UnmanagedCallbackHandlers
    {
        internal void* target;

        internal void* cacheStoreCreate;
        internal void* cacheStoreInvoke;
        internal void* cacheStoreDestroy;
        internal void* cacheStoreSessionCreate;

        internal void* cacheEntryFilterCreate;
        internal void* cacheEntryFilterApply;
        internal void* cacheEntryFilterDestroy;

        internal void* cacheInvoke;

        internal void* computeTaskMap;
        internal void* computeTaskJobResult;
        internal void* computeTaskReduce;
        internal void* computeTaskComplete;
        internal void* computeJobSerialize;
        internal void* computeJobCreate;
        internal void* computeJobExecute;
        internal void* computeJobCancel;
        internal void* computeJobDestroy;

        internal void* continuousQueryListenerApply;
        internal void* continuousQueryFilterCreate;
        internal void* continuousQueryFilterApply;
        internal void* continuousQueryFilterRelease;

        internal void* dataStreamerTopologyUpdate;
        internal void* dataStreamerStreamReceiverInvoke;
        
        internal void* futureByteResult;
        internal void* futureBoolResult;
        internal void* futureShortResult;
        internal void* futureCharResult;
        internal void* futureIntResult;
        internal void* futureFloatResult;
        internal void* futureLongResult;
        internal void* futureDoubleResult;
        internal void* futureObjectResult;
        internal void* futureNullResult;
        internal void* futureError;

        internal void* lifecycleOnEvent;

        internal void* memoryReallocate;

        internal void* messagingFilterCreate;
        internal void* messagingFilterApply;
        internal void* messagingFilterDestroy;
        
        internal void* eventFilterCreate;
        internal void* eventFilterApply;
        internal void* eventFilterDestroy;

        internal void* serviceInit;
        internal void* serviceExecute;
        internal void* serviceCancel;
        internal void* serviceInvokeMethod;

        internal void* clusterNodeFilterApply;

        internal void* nodeInfo;

        internal void* onStart;
        internal void* onStop;
        internal void* error;

        internal void* extensionCbInLongOutLong;
        internal void* extensionCbInLongLongOutLong;
    }
}
