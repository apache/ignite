/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Unmanaged
{
    /// <summary>
    /// Callback op codes.
    /// </summary>
    internal enum UnmanagedCallbackOp
    {
        CacheStoreCreate = 1,
        CacheStoreInvoke = 2,
        CacheStoreDestroy = 3,
        CacheStoreSessionCreate = 4,
        CacheEntryFilterCreate = 5,
        CacheEntryFilterApply = 6,
        CacheEntryFilterDestroy = 7,
        CacheInvoke = 8,
        ComputeTaskMap = 9,
        ComputeTaskJobResult = 10,
        ComputeTaskReduce = 11,
        ComputeTaskComplete = 12,
        ComputeJobSerialize = 13,
        ComputeJobCreate = 14,
        ComputeJobExecute = 15,
        ComputeJobCancel = 16,
        ComputeJobDestroy = 17,
        ContinuousQueryListenerApply = 18,
        ContinuousQueryFilterCreate = 19,
        ContinuousQueryFilterApply = 20,
        ContinuousQueryFilterRelease = 21,
        DataStreamerTopologyUpdate = 22,
        DataStreamerStreamReceiverInvoke = 23,
        FutureByteResult = 24,
        FutureBoolResult = 25,
        FutureShortResult = 26,
        FutureCharResult = 27,
        FutureIntResult = 28,
        FutureFloatResult = 29,
        FutureLongResult = 30,
        FutureDoubleResult = 31,
        FutureObjectResult = 32,
        FutureNullResult = 33,
        FutureError = 34,
        LifecycleOnEvent = 35,
        MemoryReallocate = 36,
        MessagingFilterCreate = 37,
        MessagingFilterApply = 38,
        MessagingFilterDestroy = 39,
        EventFilterCreate = 40,
        EventFilterApply = 41,
        EventFilterDestroy = 42,
        ServiceInit = 43,
        ServiceExecute = 44,
        ServiceCancel = 45,
        ServiceInvokeMethod = 46,
        ClusterNodeFilterApply = 47,
        NodeInfo = 48,
        OnStart = 49,
        OnStop = 50,
        ExtensionInLongLongOutLong = 52,
        OnClientDisconnected = 53,
        OnClientReconnected = 54,
        AffinityFunctionInit = 55,
        AffinityFunctionPartition = 56,
        AffinityFunctionAssignPartitions = 57,
        AffinityFunctionRemoveNode = 58,
        AffinityFunctionDestroy = 59,
        ComputeTaskLocalJobResult = 60,
        ComputeJobExecuteLocal = 61,
        PluginProcessorStop = 62,
        PluginProcessorIgniteStop = 63,
        PluginCallbackInLongLongOutLong = 68,
        EventLocalListenerApply = 69
    }
}
