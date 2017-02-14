/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License; Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; software
 * distributed under the License is distributed on an "AS IS" BASIS;
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.callback;

/**
 * Platform callback operation codes.
 */
class PlatformCallbackOp {
    /** */
    public static final int CacheStoreCreate = 1;

    /** */
    public static final int CacheStoreInvoke = 2;

    /** */
    public static final int CacheStoreDestroy = 3;

    /** */
    public static final int CacheStoreSessionCreate = 4;

    /** */
    public static final int CacheEntryFilterCreate = 5;

    /** */
    public static final int CacheEntryFilterApply = 6;

    /** */
    public static final int CacheEntryFilterDestroy = 7;

    /** */
    public static final int CacheInvoke = 8;

    /** */
    public static final int ComputeTaskMap = 9;

    /** */
    public static final int ComputeTaskJobResult = 10;

    /** */
    public static final int ComputeTaskReduce = 11;

    /** */
    public static final int ComputeTaskComplete = 12;

    /** */
    public static final int ComputeJobSerialize = 13;

    /** */
    public static final int ComputeJobCreate = 14;

    /** */
    public static final int ComputeJobExecute = 15;

    /** */
    public static final int ComputeJobCancel = 16;

    /** */
    public static final int ComputeJobDestroy = 17;

    /** */
    public static final int ContinuousQueryListenerApply = 18;

    /** */
    public static final int ContinuousQueryFilterCreate = 19;

    /** */
    public static final int ContinuousQueryFilterApply = 20;

    /** */
    public static final int ContinuousQueryFilterRelease = 21;

    /** */
    public static final int DataStreamerTopologyUpdate = 22;

    /** */
    public static final int DataStreamerStreamReceiverInvoke = 23;

    /** */
    public static final int FutureByteResult = 24;

    /** */
    public static final int FutureBoolResult = 25;

    /** */
    public static final int FutureShortResult = 26;

    /** */
    public static final int FutureCharResult = 27;

    /** */
    public static final int FutureIntResult = 28;

    /** */
    public static final int FutureFloatResult = 29;

    /** */
    public static final int FutureLongResult = 30;

    /** */
    public static final int FutureDoubleResult = 31;

    /** */
    public static final int FutureObjectResult = 32;

    /** */
    public static final int FutureNullResult = 33;

    /** */
    public static final int FutureError = 34;

    /** */
    public static final int LifecycleOnEvent = 35;

    /** */
    public static final int MemoryReallocate = 36;

    /** */
    public static final int MessagingFilterCreate = 37;

    /** */
    public static final int MessagingFilterApply = 38;

    /** */
    public static final int MessagingFilterDestroy = 39;

    /** */
    public static final int EventFilterCreate = 40;

    /** */
    public static final int EventFilterApply = 41;

    /** */
    public static final int EventFilterDestroy = 42;

    /** */
    public static final int ServiceInit = 43;

    /** */
    public static final int ServiceExecute = 44;

    /** */
    public static final int ServiceCancel = 45;

    /** */
    public static final int ServiceInvokeMethod = 46;

    /** */
    public static final int ClusterNodeFilterApply = 47;

    /** */
    public static final int NodeInfo = 48;

    /** */
    public static final int OnStart = 49;

    /** */
    public static final int OnStop = 50;

    /** */
    public static final int ExtensionInLongOutLong = 51;

    /** */
    public static final int ExtensionInLongLongOutLong = 52;

    /** */
    public static final int OnClientDisconnected = 53;

    /** */
    public static final int OnClientReconnected = 54;

    /** */
    public static final int AffinityFunctionInit = 55;

    /** */
    public static final int AffinityFunctionPartition = 56;

    /** */
    public static final int AffinityFunctionAssignPartitions = 57;

    /** */
    public static final int AffinityFunctionRemoveNode = 58;

    /** */
    public static final int AffinityFunctionDestroy = 59;

    /** */
    public static final int ComputeTaskLocalJobResult = 60;

    /** */
    public static final int ComputeJobExecuteLocal = 61;
}
