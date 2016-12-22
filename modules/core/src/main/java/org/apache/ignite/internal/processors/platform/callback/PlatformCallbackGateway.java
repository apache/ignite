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

package org.apache.ignite.internal.processors.platform.callback;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.platform.PlatformTargetProxy;
import org.apache.ignite.internal.util.GridStripedSpinBusyLock;

/**
 * Gateway to all platform-dependent callbacks. Implementers might extend this class and provide additional callbacks.
 */
@SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
public class PlatformCallbackGateway {
    /** Environment pointer. */
    protected final long envPtr;

    /** Lock. */
    private final GridStripedSpinBusyLock lock = new GridStripedSpinBusyLock();

    /**
     * Native gateway.
     *
     * @param envPtr Environment pointer.
     */
    public PlatformCallbackGateway(long envPtr) {
        this.envPtr = envPtr;
    }

    /**
     * Get environment pointer.
     *
     * @return Environment pointer.
     */
    public long environmentPointer() {
        return envPtr;
    }

    /**
     * Create cache store.
     *
     * @param memPtr Memory pointer.
     * @return Pointer.
     */
    public long cacheStoreCreate(long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.CacheStoreCreate, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * @param memPtr Memory pointer.
     * @return Result.
     */
    public int cacheStoreInvoke(long memPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.CacheStoreInvoke, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * @param objPtr Object pointer.
     */
    public void cacheStoreDestroy(long objPtr) {
        if (!lock.enterBusy())
            return;  // no need to destroy stores on grid stop

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.CacheStoreDestroy, objPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Creates cache store session.
     *
     * @return Session instance pointer.
     */
    public long cacheStoreSessionCreate() {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.CacheStoreSessionCreate, 0);
        }
        finally {
            leave();
        }
    }

    /**
     * Creates cache entry filter and returns a pointer.
     *
     * @param memPtr Memory pointer.
     * @return Pointer.
     */
    public long cacheEntryFilterCreate(long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.CacheEntryFilterCreate, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * @param memPtr Memory pointer.
     * @return Result.
     */
    public int cacheEntryFilterApply(long memPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.CacheEntryFilterApply, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * @param ptr Pointer.
     */
    public void cacheEntryFilterDestroy(long ptr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.CacheEntryFilterDestroy, ptr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invoke cache entry processor.
     *
     * @param memPtr Memory pointer.
     */
    public void cacheInvoke(long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.CacheInvoke, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Perform native task map. Do not throw exceptions, serializing them to the output stream instead.
     *
     * @param memPtr Memory pointer.
     */
    public void computeTaskMap(long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ComputeTaskMap, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Perform native task job result notification.
     *
     * @param taskPtr Task pointer.
     * @param jobPtr Job pointer.
     * @return Job result enum ordinal.
     */
    public int computeTaskLocalJobResult(long taskPtr, long jobPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.ComputeTaskLocalJobResult, taskPtr, jobPtr, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Perform native task job result notification.
     *
     * @param memPtr Memory pointer.
     * @return Job result enum ordinal.
     */
    public int computeTaskJobResult(long memPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ComputeTaskJobResult, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Perform native task reduce.
     *
     * @param taskPtr Task pointer.
     */
    public void computeTaskReduce(long taskPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ComputeTaskReduce, taskPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Complete task with native error.
     *
     * @param taskPtr Task pointer.
     * @param memPtr Memory pointer with exception data or {@code 0} in case of success.
     */
    public void computeTaskComplete(long taskPtr, long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.ComputeTaskComplete, taskPtr, memPtr, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Serialize native job.
     *
     * @param jobPtr Job pointer.
     * @param memPtr Memory pointer.
     * @return {@code True} if serialization succeeded.
     */
    public int computeJobSerialize(long jobPtr, long memPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.ComputeJobSerialize, jobPtr, memPtr, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Create job in native platform.
     *
     * @param memPtr Memory pointer.
     * @return Pointer to job.
     */
    public long computeJobCreate(long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ComputeJobCreate, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Execute native job on a node other than where it was created.
     *
     * @param jobPtr Job pointer.
     * @param cancel Cancel flag.
     */
    public void computeJobExecuteLocal(long jobPtr, long cancel) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.ComputeJobExecuteLocal, jobPtr, cancel, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Execute native job on a node other than where it was created.
     *
     * @param memPtr Memory pointer.
     */
    public void computeJobExecute(long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ComputeJobExecute, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Cancel the job.
     *
     * @param jobPtr Job pointer.
     */
    public void computeJobCancel(long jobPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ComputeJobCancel, jobPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Destroy the job.
     *
     * @param ptr Pointer.
     */
    public void computeJobDestroy(long ptr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ComputeJobDestroy, ptr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invoke local callback.
     *
     * @param memPtr Memory pointer.
     */
    public void continuousQueryListenerApply(long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ContinuousQueryListenerApply, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Create filter in native platform.
     *
     * @param memPtr Memory pointer.
     * @return Pointer to created filter.
     */
    public long continuousQueryFilterCreate(long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ContinuousQueryFilterCreate, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invoke remote filter.
     *
     * @param memPtr Memory pointer.
     * @return Result.
     */
    public long continuousQueryFilterApply(long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ContinuousQueryFilterApply, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Release remote  filter.
     *
     * @param filterPtr Filter pointer.
     */
    public void continuousQueryFilterRelease(long filterPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr,
                PlatformCallbackOp.ContinuousQueryFilterRelease, filterPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify native data streamer about topology update.
     *
     * @param ptr Data streamer native pointer.
     * @param topVer Topology version.
     * @param topSize Topology size.
     */
    public void dataStreamerTopologyUpdate(long ptr, long topVer, int topSize) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.DataStreamerTopologyUpdate, ptr, topVer, topSize, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Invoke stream receiver.
     *
     * @param cache Cache object.
     * @param memPtr Stream pointer.
     */
    public void dataStreamerStreamReceiverInvoke(long ptr, PlatformTargetProxy cache, long memPtr, boolean keepBinary) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.DataStreamerStreamReceiverInvoke, memPtr, 0, 0, cache);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with byte result.
     *
     * @param futPtr Future pointer.
     * @param res Result.
     */
    public void futureByteResult(long futPtr, long res) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureByteResult, futPtr, res, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with boolean result.
     *
     * @param futPtr Future pointer.
     * @param res Result.
     */
    public void futureBoolResult(long futPtr, long res) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureBoolResult, futPtr, res, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with short result.
     *
     * @param futPtr Future pointer.
     * @param res Result.
     */
    public void futureShortResult(long futPtr, long res) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureShortResult, futPtr, res, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with byte result.
     *
     * @param futPtr Future pointer.
     * @param res Result.
     */
    public void futureCharResult(long futPtr, long res) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureCharResult, futPtr, res, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with int result.
     *
     * @param futPtr Future pointer.
     * @param res Result.
     */
    public void futureIntResult(long futPtr, long res) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureIntResult, futPtr, res, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with float result.
     *
     * @param futPtr Future pointer.
     * @param res Result.
     */
    public void futureFloatResult(long futPtr, long res) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureFloatResult, futPtr, res, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with long result.
     *
     * @param futPtr Future pointer.
     * @param res Result.
     */
    public void futureLongResult(long futPtr, long res) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureLongResult, futPtr, res, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with double result.
     *
     * @param futPtr Future pointer.
     * @param res Result.
     */
    public void futureDoubleResult(long futPtr, long res) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureDoubleResult, futPtr, res, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with object result.
     *
     * @param futPtr Future pointer.
     * @param memPtr Memory pointer.
     */
    public void futureObjectResult(long futPtr, long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureObjectResult, futPtr, memPtr, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with null result.
     *
     * @param futPtr Future pointer.
     */
    public void futureNullResult(long futPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.FutureNullResult, futPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Notify future with error.
     *
     * @param futPtr Future pointer.
     * @param memPtr Pointer to memory with error information.
     */
    public void futureError(long futPtr, long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.FutureError, futPtr, memPtr, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Creates message filter and returns a pointer.
     *
     * @param memPtr Memory pointer.
     * @return Pointer.
     */
    public long messagingFilterCreate(long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr,
                PlatformCallbackOp.MessagingFilterCreate, memPtr);
       }
        finally {
            leave();
        }
    }

    /**
     * @param ptr Pointer.
     * @param memPtr Memory pointer.
     * @return Result.
     */
    public int messagingFilterApply(long ptr, long memPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.MessagingFilterApply, ptr, memPtr, 0, null);
        }
        finally {
            leave();
        }}

    /**
     * @param ptr Pointer.
     */
    public void messagingFilterDestroy(long ptr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.MessagingFilterDestroy, ptr);
        }
        finally {
            leave();
        }
    }

    /**
     * Creates event filter and returns a pointer.
     *
     * @param memPtr Memory pointer.
     * @return Pointer.
     */
    public long eventFilterCreate(long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.EventFilterCreate, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * @param ptr Pointer.
     * @param memPtr Memory pointer.
     * @return Result.
     */
    public int eventFilterApply(long ptr, long memPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.EventFilterApply, ptr, memPtr, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * @param ptr Pointer.
     */
    public void eventFilterDestroy(long ptr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.EventFilterDestroy, ptr);
        }
        finally {
            leave();
        }
    }

    /**
     * Sends node info to native target.
     *
     * @param memPtr Ptr to a stream with serialized node.
     */
    public void nodeInfo(long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.NodeInfo, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Kernal start callback.
     *
     * @param proc Platform processor.
     * @param memPtr Memory pointer.
     */
    public void onStart(Object proc, long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr, PlatformCallbackOp.OnStart, memPtr, 0, 0, proc);
        }
        finally {
            leave();
        }
    }

    /**
     * Lifecycle event callback.
     *
     * @param ptr Holder pointer.
     * @param evt Event.
     */
    public void lifecycleEvent(long ptr, int evt) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.LifecycleOnEvent, ptr, evt, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Re-allocate external memory chunk.
     *
     * @param memPtr Cross-platform pointer.
     * @param cap Capacity.
     */
    public void memoryReallocate(long memPtr, int cap) {
        enter();

        try {
            PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.MemoryReallocate, memPtr, cap, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Initializes native service.
     *
     * @param memPtr Pointer.
     * @throws IgniteCheckedException In case of error.
     */
    public long serviceInit(long memPtr) throws IgniteCheckedException {
        enter();

        try {
            return PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ServiceInit, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Executes native service.
     *
     * @param memPtr Stream pointer.
     * @throws IgniteCheckedException In case of error.
     */
    public void serviceExecute(long memPtr) throws IgniteCheckedException {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ServiceExecute, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Cancels native service.
     *
     * @param memPtr Stream pointer.
     * @throws IgniteCheckedException In case of error.
     */
    public void serviceCancel(long memPtr) throws IgniteCheckedException {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ServiceCancel, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invokes service method.
     *
     * @param memPtr Memory pointer.
     * @throws IgniteCheckedException In case of error.
     */
    public void serviceInvokeMethod(long memPtr) throws IgniteCheckedException {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ServiceInvokeMethod, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invokes cluster node filter.
     *
     * @param memPtr Stream pointer.
     */
    public int clusterNodeFilterApply(long memPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.ClusterNodeFilterApply, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Extension callback accepting single long argument and returning long result.
     *
     * @param typ Operation type.
     * @param arg1 Argument 1.
     * @return Long result.
     */
    public long extensionCallbackInLongOutLong(int typ, long arg1) {
        enter();

        try {
            return PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.ExtensionInLongOutLong, typ, arg1, 0, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Extension callback accepting two long arguments and returning long result.
     *
     * @param typ Operation type.
     * @param arg1 Argument 1.
     * @param arg2 Argument 2.
     * @return Long result.
     */
    public long extensionCallbackInLongLongOutLong(int typ, long arg1, long arg2) {
        enter();

        try {
            return PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr,
                PlatformCallbackOp.ExtensionInLongLongOutLong, typ, arg1, arg2, null);
        }
        finally {
            leave();
        }
    }

    /**
     * Notifies platform about client disconnect.
     */
    public void onClientDisconnected() {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.OnClientDisconnected, 0);
        }
        finally {
            leave();
        }
    }

    /**
     * Notifies platform about client reconnect.
     *
     * @param clusterRestarted Cluster restarted flag.
     */
    public void onClientReconnected(boolean clusterRestarted) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr,
                PlatformCallbackOp.OnClientReconnected, clusterRestarted ? 1 : 0);
        }
        finally {
            leave();
        }
    }

    /**
     * Logs to the platform.
     *
     * @param level Log level.
     * @param message Message.
     * @param category Category.
     * @param errorInfo Error info.
     * @param memPtr Pointer to optional payload (serialized exception).
     */
    public void loggerLog(int level, String message, String category, String errorInfo, long memPtr) {
        if (!tryEnter())
            return;  // Do not lock for logger: this should work during shutdown

        try {
            PlatformCallbackUtils.loggerLog(envPtr, level, message, category, errorInfo, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Gets a value indicating whether native logger has specified level enabled.
     *
     * @param level Log level.
     */
    public boolean loggerIsLevelEnabled(int level) {
        if (!tryEnter())
            return false;  // Do not lock for logger: this should work during shutdown

        try {
            return PlatformCallbackUtils.loggerIsLevelEnabled(envPtr, level);
        }
        finally {
            leave();
        }
    }

    /**
     * Kernal stop callback.
     */
    public void onStop() {
        block();

        PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.OnStop, 0);
    }

    /**
     * Initializes affinity function.
     *
     * @param memPtr Pointer to a stream with serialized affinity function.
     * @param baseFunc Optional func for base calls.
     * @return Affinity function pointer.
     */
    public long affinityFunctionInit(long memPtr, PlatformTargetProxy baseFunc) {
        enter();

        try {
            return PlatformCallbackUtils.inLongLongLongObjectOutLong(envPtr, PlatformCallbackOp.AffinityFunctionInit,
                memPtr, 0, 0, baseFunc);
        }
        finally {
            leave();
        }
    }

    /**
     * Gets the partition from affinity function.
     *
     * @param memPtr Pointer to a stream with data.
     * @return Partition number for a given key.
     */
    public int affinityFunctionPartition(long memPtr) {
        enter();

        try {
            return (int)PlatformCallbackUtils.inLongOutLong(envPtr,
                PlatformCallbackOp.AffinityFunctionPartition, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Assigns the affinity partitions.
     *
     * @param memPtr Pointer to a stream.
     */
    public void affinityFunctionAssignPartitions(long memPtr){
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.AffinityFunctionAssignPartitions, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Removes the node from affinity function.
     *
     * @param memPtr Pointer to a stream.
     */
    public void affinityFunctionRemoveNode(long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.AffinityFunctionRemoveNode, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Destroys the affinity function.
     *
     * @param ptr Affinity function pointer.
     */
    public void affinityFunctionDestroy(long ptr) {
        if (!lock.enterBusy())
            return;  // skip: destroy is not necessary during shutdown.

        try {
            PlatformCallbackUtils.inLongOutLong(envPtr, PlatformCallbackOp.AffinityFunctionDestroy, ptr);
        }
        finally {
            leave();
        }
    }

    /**
     * Redirects the console output to platform.
     *
     * @param str String to write.
     * @param isErr Whether this is stdErr or stdOut.
     */
    public static void consoleWrite(String str, boolean isErr) {
        PlatformCallbackUtils.consoleWrite(str, isErr);
    }

    /**
     * Enter gateway.
     */
    protected void enter() {
        if (!lock.enterBusy())
            throw new IgniteException("Failed to execute native callback because grid is stopping.");
    }

    /**
     * Enter gateway.
     */
    private boolean tryEnter() {
        return lock.enterBusy();
    }

    /**
     * Leave gateway.
     */
    protected void leave() {
        lock.leaveBusy();
    }

    /**
     * Block gateway.
     */
    protected void block() {
        lock.block();
    }
}
