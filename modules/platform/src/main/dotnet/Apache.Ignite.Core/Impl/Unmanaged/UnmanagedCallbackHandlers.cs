/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
