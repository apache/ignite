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

/**
 * Platform callback utility methods. Implemented in target platform. All methods in this class must be
 * package-visible and invoked only through {@link PlatformCallbackGateway}.
 */
public class PlatformCallbackUtils {
    /**
     * Create cache store.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Memory pointer.
     * @return Pointer.
     */
    static native long cacheStoreCreate(long envPtr, long memPtr);

    /**
     * @param envPtr Environment pointer.
     * @param objPtr Object pointer.
     * @param memPtr Memory pointer.
     * @param cb Callback.
     * @return Result.
     */
    static native int cacheStoreInvoke(long envPtr, long objPtr, long memPtr, Object cb);

    /**
     * @param envPtr Environment pointer.
     * @param objPtr Object pointer.
     */
    static native void cacheStoreDestroy(long envPtr, long objPtr);

    /**
     * Creates cache store session.
     *
     * @param envPtr Environment pointer.
     * @param storePtr Store instance pointer.
     * @return Session instance pointer.
     */
    static native long cacheStoreSessionCreate(long envPtr, long storePtr);

    /**
     * Creates cache entry filter and returns a pointer.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Memory pointer.
     * @return Pointer.
     */
    static native long cacheEntryFilterCreate(long envPtr, long memPtr);

    /**
     * @param envPtr Environment pointer.
     * @param objPtr Pointer.
     * @param memPtr Memory pointer.
     * @return Result.
     */
    static native int cacheEntryFilterApply(long envPtr, long objPtr, long memPtr);

    /**
     * @param envPtr Environment pointer.
     * @param objPtr Pointer.
     */
    static native void cacheEntryFilterDestroy(long envPtr, long objPtr);

    /**
     * Invoke cache entry processor.
     *
     * @param envPtr Environment pointer.
     * @param outMemPtr Output memory pointer.
     * @param inMemPtr Input memory pointer.
     */
    static native void cacheInvoke(long envPtr, long outMemPtr, long inMemPtr);

    /**
     * Perform native task map. Do not throw exceptions, serializing them to the output stream instead.
     *
     * @param envPtr Environment pointer.
     * @param taskPtr Task pointer.
     * @param outMemPtr Output memory pointer (exists if topology changed, otherwise {@code 0}).
     * @param inMemPtr Input memory pointer.
     */
    static native void computeTaskMap(long envPtr, long taskPtr, long outMemPtr, long inMemPtr);

    /**
     * Perform native task job result notification.
     *
     * @param envPtr Environment pointer.
     * @param taskPtr Task pointer.
     * @param jobPtr Job pointer.
     * @param memPtr Memory pointer (always zero for local job execution).
     * @return Job result enum ordinal.
     */
    static native int computeTaskJobResult(long envPtr, long taskPtr, long jobPtr, long memPtr);

    /**
     * Perform native task reduce.
     *
     * @param envPtr Environment pointer.
     * @param taskPtr Task pointer.
     */
    static native void computeTaskReduce(long envPtr, long taskPtr);

    /**
     * Complete task with native error.
     *
     * @param envPtr Environment pointer.
     * @param taskPtr Task pointer.
     * @param memPtr Memory pointer with exception data or {@code 0} in case of success.
     */
    static native void computeTaskComplete(long envPtr, long taskPtr, long memPtr);

    /**
     * Serialize native job.
     *
     * @param envPtr Environment pointer.
     * @param jobPtr Job pointer.
     * @param memPtr Memory pointer.
     * @return {@code True} if serialization succeeded.
     */
    static native int computeJobSerialize(long envPtr, long jobPtr, long memPtr);

    /**
     * Create job in native platform.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Memory pointer.
     * @return Pointer to job.
     */
    static native long computeJobCreate(long envPtr, long memPtr);

    /**
     * Execute native job on a node other than where it was created.
     *
     * @param envPtr Environment pointer.
     * @param jobPtr Job pointer.
     * @param cancel Cancel flag.
     * @param memPtr Memory pointer to write result to for remote job execution or {@code 0} for local job execution.
     */
    static native void computeJobExecute(long envPtr, long jobPtr, int cancel, long memPtr);

    /**
     * Cancel the job.
     *
     * @param envPtr Environment pointer.
     * @param jobPtr Job pointer.
     */
    static native void computeJobCancel(long envPtr, long jobPtr);

    /**
     * Destroy the job.
     *
     * @param envPtr Environment pointer.
     * @param ptr Pointer.
     */
    static native void computeJobDestroy(long envPtr, long ptr);

    /**
     * Invoke local callback.
     *
     * @param envPtr Environment pointer.
     * @param cbPtr Callback pointer.
     * @param memPtr Memory pointer.
     */
    static native void continuousQueryListenerApply(long envPtr, long cbPtr, long memPtr);

    /**
     * Create filter in native platform.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Memory pointer.
     * @return Pointer to created filter.
     */
    static native long continuousQueryFilterCreate(long envPtr, long memPtr);

    /**
     * Invoke remote filter.
     *
     * @param envPtr Environment pointer.
     * @param filterPtr Filter pointer.
     * @param memPtr Memory pointer.
     * @return Result.
     */
    static native int continuousQueryFilterApply(long envPtr, long filterPtr, long memPtr);

    /**
     * Release remote  filter.
     *
     * @param envPtr Environment pointer.
     * @param filterPtr Filter pointer.
     */
    static native void continuousQueryFilterRelease(long envPtr, long filterPtr);

    /**
     * Notify native data streamer about topology update.
     *
     * @param envPtr Environment pointer.
     * @param ptr Data streamer native pointer.
     * @param topVer Topology version.
     * @param topSize Topology size.
     */
    static native void dataStreamerTopologyUpdate(long envPtr, long ptr, long topVer, int topSize);

    /**
     * Invoke stream receiver.
     *
     * @param envPtr Environment pointer.
     * @param ptr Receiver native pointer.
     * @param cache Cache object.
     * @param memPtr Stream pointer.
     * @param keepPortable Portable flag.
     */
    static native void dataStreamerStreamReceiverInvoke(long envPtr, long ptr, Object cache, long memPtr,
        boolean keepPortable);

    /**
     * Notify future with byte result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param res Result.
     */
    static native void futureByteResult(long envPtr, long futPtr, int res);

    /**
     * Notify future with boolean result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param res Result.
     */
    static native void futureBoolResult(long envPtr, long futPtr, int res);

    /**
     * Notify future with short result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param res Result.
     */
    static native void futureShortResult(long envPtr, long futPtr, int res);

    /**
     * Notify future with byte result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param res Result.
     */
    static native void futureCharResult(long envPtr, long futPtr, int res);

    /**
     * Notify future with int result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param res Result.
     */
    static native void futureIntResult(long envPtr, long futPtr, int res);

    /**
     * Notify future with float result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param res Result.
     */
    static native void futureFloatResult(long envPtr, long futPtr, float res);

    /**
     * Notify future with long result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param res Result.
     */
    static native void futureLongResult(long envPtr, long futPtr, long res);

    /**
     * Notify future with double result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param res Result.
     */
    static native void futureDoubleResult(long envPtr, long futPtr, double res);

    /**
     * Notify future with object result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param memPtr Memory pointer.
     */
    static native void futureObjectResult(long envPtr, long futPtr, long memPtr);

    /**
     * Notify future with null result.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     */
    static native void futureNullResult(long envPtr, long futPtr);

    /**
     * Notify future with error.
     *
     * @param envPtr Environment pointer.
     * @param futPtr Future pointer.
     * @param memPtr Pointer to memory with error information.
     */
    static native void futureError(long envPtr, long futPtr, long memPtr);

    /**
     * Creates message filter and returns a pointer.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Memory pointer.
     * @return Pointer.
     */
    static native long messagingFilterCreate(long envPtr, long memPtr);

    /**
     * @param envPtr Environment pointer.
     * @param objPtr Pointer.
     * @param memPtr Memory pointer.
     * @return Result.
     */
    static native int messagingFilterApply(long envPtr, long objPtr, long memPtr);

    /**
     * @param envPtr Environment pointer.
     * @param objPtr Pointer.
     */
    static native void messagingFilterDestroy(long envPtr, long objPtr);

    /**
     * Creates event filter and returns a pointer.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Memory pointer.
     * @return Pointer.
     */
    static native long eventFilterCreate(long envPtr, long memPtr);

    /**
     * @param envPtr Environment pointer.
     * @param objPtr Pointer.
     * @param memPtr Memory pointer.
     * @return Result.
     */
    static native int eventFilterApply(long envPtr, long objPtr, long memPtr);

    /**
     * @param envPtr Environment pointer.
     * @param objPtr Pointer.
     */
    static native void eventFilterDestroy(long envPtr, long objPtr);

    /**
     * Sends node info to native target.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Ptr to a stream with serialized node.
     */
    static native void nodeInfo(long envPtr, long memPtr);

    /**
     * Kernal start callback.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Memory pointer.
     */
    static native void onStart(long envPtr, long memPtr);

    /*
     * Kernal stop callback.
     *
     * @param envPtr Environment pointer.
     */
    static native void onStop(long envPtr);

    /**
     * Lifecycle event callback.
     *
     * @param envPtr Environment pointer.
     * @param ptr Holder pointer.
     * @param evt Event.
     */
    static native void lifecycleEvent(long envPtr, long ptr, int evt);

    /**
     * Re-allocate external memory chunk.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Cross-platform pointer.
     * @param cap Capacity.
     */
    static native void memoryReallocate(long envPtr, long memPtr, int cap);

    /**
     * Initializes native service.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Stream pointer.
     * @return Pointer to the native platform service.
     */
    static native long serviceInit(long envPtr, long memPtr);

    /**
     * Executes native service.
     *
     * @param envPtr Environment pointer.
     * @param svcPtr Pointer to the service in the native platform.
     * @param memPtr Stream pointer.
     */
    static native void serviceExecute(long envPtr, long svcPtr, long memPtr);

    /**
     * Cancels native service.
     *
     * @param envPtr Environment pointer.
     * @param svcPtr Pointer to the service in the native platform.
     * @param memPtr Stream pointer.
     */
    static native void serviceCancel(long envPtr, long svcPtr, long memPtr);

    /**
     /**
     * Invokes service method.
     *
     * @param envPtr Environment pointer.
     * @param svcPtr Pointer to the service in the native platform.
     * @param outMemPtr Output memory pointer.
     * @param inMemPtr Input memory pointer.
     */
    static native void serviceInvokeMethod(long envPtr, long svcPtr, long outMemPtr, long inMemPtr);

    /**
     * Invokes cluster node filter.
     *
     * @param envPtr Environment pointer.
     * @param memPtr Stream pointer.
     */
    static native int clusterNodeFilterApply(long envPtr, long memPtr);

    /**
     * Private constructor.
     */
    private PlatformCallbackUtils() {
        // No-op.
    }
}
