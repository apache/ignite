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
            return PlatformCallbackUtils.cacheStoreCreate(envPtr, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * @param objPtr Object pointer.
     * @param memPtr Memory pointer.
     * @return Result.
     */
    public int cacheStoreInvoke(long objPtr, long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.cacheStoreInvoke(envPtr, objPtr, memPtr);
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
            PlatformCallbackUtils.cacheStoreDestroy(envPtr, objPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Creates cache store session.
     *
     * @param storePtr Store instance pointer.
     * @return Session instance pointer.
     */
    public long cacheStoreSessionCreate(long storePtr) {
        enter();

        try {
            return PlatformCallbackUtils.cacheStoreSessionCreate(envPtr, storePtr);
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
            return PlatformCallbackUtils.cacheEntryFilterCreate(envPtr, memPtr);
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
    public int cacheEntryFilterApply(long ptr, long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.cacheEntryFilterApply(envPtr, ptr, memPtr);
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
            PlatformCallbackUtils.cacheEntryFilterDestroy(envPtr, ptr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invoke cache entry processor.
     *
     * @param outMemPtr Output memory pointer.
     * @param inMemPtr Input memory pointer.
     */
    public void cacheInvoke(long outMemPtr, long inMemPtr) {
        enter();

        try {
            PlatformCallbackUtils.cacheInvoke(envPtr, outMemPtr, inMemPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Perform native task map. Do not throw exceptions, serializing them to the output stream instead.
     *
     * @param taskPtr Task pointer.
     * @param outMemPtr Output memory pointer (exists if topology changed, otherwise {@code 0}).
     * @param inMemPtr Input memory pointer.
     */
    public void computeTaskMap(long taskPtr, long outMemPtr, long inMemPtr) {
        enter();

        try {
            PlatformCallbackUtils.computeTaskMap(envPtr, taskPtr, outMemPtr, inMemPtr);
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
     * @param memPtr Memory pointer (always zero for local job execution).
     * @return Job result enum ordinal.
     */
    public int computeTaskJobResult(long taskPtr, long jobPtr, long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.computeTaskJobResult(envPtr, taskPtr, jobPtr, memPtr);
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
            PlatformCallbackUtils.computeTaskReduce(envPtr, taskPtr);
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
            PlatformCallbackUtils.computeTaskComplete(envPtr, taskPtr, memPtr);
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
            return PlatformCallbackUtils.computeJobSerialize(envPtr, jobPtr, memPtr);
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
            return PlatformCallbackUtils.computeJobCreate(envPtr, memPtr);
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
     * @param memPtr Memory pointer to write result to for remote job execution or {@code 0} for local job execution.
     */
    public void computeJobExecute(long jobPtr, int cancel, long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.computeJobExecute(envPtr, jobPtr, cancel, memPtr);
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
            PlatformCallbackUtils.computeJobCancel(envPtr, jobPtr);
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
            PlatformCallbackUtils.computeJobDestroy(envPtr, ptr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invoke local callback.
     *
     * @param cbPtr Callback pointer.
     * @param memPtr Memory pointer.
     */
    public void continuousQueryListenerApply(long cbPtr, long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.continuousQueryListenerApply(envPtr, cbPtr, memPtr);
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
            return PlatformCallbackUtils.continuousQueryFilterCreate(envPtr, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invoke remote filter.
     *
     * @param filterPtr Filter pointer.
     * @param memPtr Memory pointer.
     * @return Result.
     */
    public int continuousQueryFilterApply(long filterPtr, long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.continuousQueryFilterApply(envPtr, filterPtr, memPtr);
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
            PlatformCallbackUtils.continuousQueryFilterRelease(envPtr, filterPtr);
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
            PlatformCallbackUtils.dataStreamerTopologyUpdate(envPtr, ptr, topVer, topSize);
        }
        finally {
            leave();
        }
    }

    /**
     * Invoke stream receiver.
     *
     * @param ptr Receiver native pointer.
     * @param cache Cache object.
     * @param memPtr Stream pointer.
     * @param keepBinary Binary flag.
     */
    public void dataStreamerStreamReceiverInvoke(long ptr, PlatformTargetProxy cache, long memPtr, boolean keepBinary) {
        enter();

        try {
            PlatformCallbackUtils.dataStreamerStreamReceiverInvoke(envPtr, ptr, cache, memPtr, keepBinary);
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
    public void futureByteResult(long futPtr, int res) {
        enter();

        try {
            PlatformCallbackUtils.futureByteResult(envPtr, futPtr, res);
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
    public void futureBoolResult(long futPtr, int res) {
        enter();

        try {
            PlatformCallbackUtils.futureBoolResult(envPtr, futPtr, res);
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
    public void futureShortResult(long futPtr, int res) {
        enter();

        try {
            PlatformCallbackUtils.futureShortResult(envPtr, futPtr, res);
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
    public void futureCharResult(long futPtr, int res) {
        enter();

        try {
            PlatformCallbackUtils.futureCharResult(envPtr, futPtr, res);
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
    public void futureIntResult(long futPtr, int res) {
        enter();

        try {
            PlatformCallbackUtils.futureIntResult(envPtr, futPtr, res);
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
    public void futureFloatResult(long futPtr, float res) {
        enter();

        try {
            PlatformCallbackUtils.futureFloatResult(envPtr, futPtr, res);
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
            PlatformCallbackUtils.futureLongResult(envPtr, futPtr, res);
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
    public void futureDoubleResult(long futPtr, double res) {
        enter();

        try {
            PlatformCallbackUtils.futureDoubleResult(envPtr, futPtr, res);
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
            PlatformCallbackUtils.futureObjectResult(envPtr, futPtr, memPtr);
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
            PlatformCallbackUtils.futureNullResult(envPtr, futPtr);
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
            PlatformCallbackUtils.futureError(envPtr, futPtr, memPtr);
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
            return PlatformCallbackUtils.messagingFilterCreate(envPtr, memPtr);
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
            return PlatformCallbackUtils.messagingFilterApply(envPtr, ptr, memPtr);
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
            PlatformCallbackUtils.messagingFilterDestroy(envPtr, ptr);
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
            return PlatformCallbackUtils.eventFilterCreate(envPtr, memPtr);
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
            return PlatformCallbackUtils.eventFilterApply(envPtr, ptr, memPtr);
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
            PlatformCallbackUtils.eventFilterDestroy(envPtr, ptr);
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
            PlatformCallbackUtils.nodeInfo(envPtr, memPtr);
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
            PlatformCallbackUtils.onStart(envPtr, proc, memPtr);
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
            PlatformCallbackUtils.lifecycleEvent(envPtr, ptr, evt);
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
            PlatformCallbackUtils.memoryReallocate(envPtr, memPtr, cap);
        }
        finally {
            leave();
        }
    }

    /**
     * Initializes native service.
     *
     * @param memPtr Pointer.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    public long serviceInit(long memPtr) throws IgniteCheckedException {
        enter();

        try {
            return PlatformCallbackUtils.serviceInit(envPtr, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Executes native service.
     *
     * @param svcPtr Pointer to the service in the native platform.
     * @param memPtr Stream pointer.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    public void serviceExecute(long svcPtr, long memPtr) throws IgniteCheckedException {
        enter();

        try {
            PlatformCallbackUtils.serviceExecute(envPtr, svcPtr, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Cancels native service.
     *
     * @param svcPtr Pointer to the service in the native platform.
     * @param memPtr Stream pointer.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    public void serviceCancel(long svcPtr, long memPtr) throws IgniteCheckedException {
        enter();

        try {
            PlatformCallbackUtils.serviceCancel(envPtr, svcPtr, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Invokes service method.
     *
     * @param svcPtr Pointer to the service in the native platform.
     * @param outMemPtr Output memory pointer.
     * @param inMemPtr Input memory pointer.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    public void serviceInvokeMethod(long svcPtr, long outMemPtr, long inMemPtr) throws IgniteCheckedException {
        enter();

        try {
            PlatformCallbackUtils.serviceInvokeMethod(envPtr, svcPtr, outMemPtr, inMemPtr);
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
            return PlatformCallbackUtils.clusterNodeFilterApply(envPtr, memPtr);
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
            return PlatformCallbackUtils.extensionCallbackInLongOutLong(envPtr, typ, arg1);
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
            return PlatformCallbackUtils.extensionCallbackInLongLongOutLong(envPtr, typ, arg1, arg2);
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
            PlatformCallbackUtils.onClientDisconnected(envPtr);
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
            PlatformCallbackUtils.onClientReconnected(envPtr, clusterRestarted);
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

        PlatformCallbackUtils.onStop(envPtr);
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
            return PlatformCallbackUtils.affinityFunctionInit(envPtr, memPtr, baseFunc);
        }
        finally {
            leave();
        }
    }

    /**
     * Gets the partition from affinity function.
     *
     * @param ptr Affinity function pointer.
     * @param memPtr Pointer to a stream with key object.
     * @return Partition number for a given key.
     */
    public int affinityFunctionPartition(long ptr, long memPtr) {
        enter();

        try {
            return PlatformCallbackUtils.affinityFunctionPartition(envPtr, ptr, memPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Assigns the affinity partitions.
     *
     * @param ptr Affinity function pointer.
     * @param outMemPtr Pointer to a stream with affinity context.
     * @param inMemPtr Pointer to a stream with result.
     */
    public void affinityFunctionAssignPartitions(long ptr, long outMemPtr, long inMemPtr){
        enter();

        try {
            PlatformCallbackUtils.affinityFunctionAssignPartitions(envPtr, ptr, outMemPtr, inMemPtr);
        }
        finally {
            leave();
        }
    }

    /**
     * Removes the node from affinity function.
     *
     * @param ptr Affinity function pointer.
     * @param memPtr Pointer to a stream with node id.
     */
    public void affinityFunctionRemoveNode(long ptr, long memPtr) {
        enter();

        try {
            PlatformCallbackUtils.affinityFunctionRemoveNode(envPtr, ptr, memPtr);
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
            PlatformCallbackUtils.affinityFunctionDestroy(envPtr, ptr);
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
    protected boolean tryEnter() {
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
