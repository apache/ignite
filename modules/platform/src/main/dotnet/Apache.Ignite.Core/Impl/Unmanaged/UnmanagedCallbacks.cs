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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Cache.Store;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Compute;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Events;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Messaging;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Impl.Services;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Services;
    using U = GridUtils;
    using UU = UnmanagedUtils;

    /// <summary>
    /// Unmanaged callbacks.
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedMember.Local")]
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable", 
        Justification = "This class instance usually lives as long as the app runs.")]
    [SuppressMessage("Microsoft.Design", "CA1049:TypesThatOwnNativeResourcesShouldBeDisposable", 
        Justification = "This class instance usually lives as long as the app runs.")]
    internal unsafe class UnmanagedCallbacks
    {
        /** Unmanaged context. */
        private volatile UnmanagedContext ctx;

        /** Handle registry. */
        private readonly HandleRegistry handleRegistry = new HandleRegistry();
        
        /** Grid. */
        private volatile Ignite grid;

        /** Keep references to created delegates. */
        // ReSharper disable once CollectionNeverQueried.Local
        private readonly List<Delegate> delegates = new List<Delegate>(50);

        /** Initialized flag. */
        private readonly ManualResetEventSlim initEvent = new ManualResetEventSlim(false);

        /** Actions to be called upon grid initialisation. */
        private readonly List<Action<Ignite>> initActions = new List<Action<Ignite>>();

        /** GC handle to UnmanagedCallbacks instance to prevent it from being GCed. */
        private readonly GCHandle thisHnd;

        /** Callbacks pointer. */
        private readonly IntPtr cbsPtr;

        /** Error type: generic. */
        private const int ERR_GENERIC = 1;

        /** Error type: initialize. */
        private const int ERR_JVM_INIT = 2;

        /** Error type: attach. */
        private const int ERR_JVM_ATTACH = 3;

        /** Opeartion: prepare .Net. */
        private const int OP_PREPARE_DOT_NET = 1;

        /** Opeartion: DR entry filter create. */
        private const int OP_DR_ENTRY_FILTER_CREATE = 2;

        /** Opeartion: DR entry filter apply. */
        private const int OP_DR_ENTRY_FILTER_APPLY = 3;

        /** Opeartion: DR entry filter destroy. */
        private const int OP_DR_ENTRY_FILTER_DESTROY = 4;

        private delegate long CacheStoreCreateCallbackDelegate(void* target, long memPtr);
        private delegate int CacheStoreInvokeCallbackDelegate(void* target, long objPtr, long memPtr, void* cb);
        private delegate void CacheStoreDestroyCallbackDelegate(void* target, long objPtr);
        private delegate long CacheStoreSessionCreateCallbackDelegate(void* target, long storePtr);

        private delegate long CacheEntryFilterCreateCallbackDelegate(void* target, long memPtr);
        private delegate int CacheEntryFilterApplyCallbackDelegate(void* target, long objPtr, long memPtr);
        private delegate void CacheEntryFilterDestroyCallbackDelegate(void* target, long objPtr);

        private delegate void CacheInvokeCallbackDelegate(void* target, long inMemPtr, long outMemPtr);

        private delegate void ComputeTaskMapCallbackDelegate(void* target, long taskPtr, long inMemPtr, long outMemPtr);
        private delegate int ComputeTaskJobResultCallbackDelegate(void* target, long taskPtr, long jobPtr, long memPtr);
        private delegate void ComputeTaskReduceCallbackDelegate(void* target, long taskPtr);
        private delegate void ComputeTaskCompleteCallbackDelegate(void* target, long taskPtr, long memPtr);
        private delegate int ComputeJobSerializeCallbackDelegate(void* target, long jobPtr, long memPtr);
        private delegate long ComputeJobCreateCallbackDelegate(void* target, long memPtr);
        private delegate void ComputeJobExecuteCallbackDelegate(void* target, long jobPtr, int cancel, long memPtr);
        private delegate void ComputeJobCancelCallbackDelegate(void* target, long jobPtr);
        private delegate void ComputeJobDestroyCallbackDelegate(void* target, long jobPtr);

        private delegate void ContinuousQueryListenerApplyCallbackDelegate(void* target, long lsnrPtr, long memPtr);
        private delegate long ContinuousQueryFilterCreateCallbackDelegate(void* target, long memPtr);
        private delegate int ContinuousQueryFilterApplyCallbackDelegate(void* target, long filterPtr, long memPtr);
        private delegate void ContinuousQueryFilterReleaseCallbackDelegate(void* target, long filterPtr);

        private delegate void DataStreamerTopologyUpdateCallbackDelegate(void* target, long ldrPtr, long topVer, int topSize);
        private delegate void DataStreamerStreamReceiverInvokeCallbackDelegate(void* target, long ptr, void* cache, long memPtr, byte keepPortable);

        private delegate void FutureByteResultCallbackDelegate(void* target, long futPtr, int res);
        private delegate void FutureBoolResultCallbackDelegate(void* target, long futPtr, int res);
        private delegate void FutureShortResultCallbackDelegate(void* target, long futPtr, int res);
        private delegate void FutureCharResultCallbackDelegate(void* target, long futPtr, int res);
        private delegate void FutureIntResultCallbackDelegate(void* target, long futPtr, int res);
        private delegate void FutureFloatResultCallbackDelegate(void* target, long futPtr, float res);
        private delegate void FutureLongResultCallbackDelegate(void* target, long futPtr, long res);
        private delegate void FutureDoubleResultCallbackDelegate(void* target, long futPtr, double res);
        private delegate void FutureObjectResultCallbackDelegate(void* target, long futPtr, long memPtr);
        private delegate void FutureNullResultCallbackDelegate(void* target, long futPtr);
        private delegate void FutureErrorCallbackDelegate(void* target, long futPtr, long memPtr);

        private delegate void LifecycleOnEventCallbackDelegate(void* target, long ptr, int evt);

        private delegate void MemoryReallocateCallbackDelegate(void* target, long memPtr, int cap);

        private delegate long MessagingFilterCreateCallbackDelegate(void* target, long memPtr);
        private delegate int MessagingFilterApplyCallbackDelegate(void* target, long ptr, long memPtr);
        private delegate void MessagingFilterDestroyCallbackDelegate(void* target, long ptr);
        
        private delegate long EventFilterCreateCallbackDelegate(void* target, long memPtr);
        private delegate int EventFilterApplyCallbackDelegate(void* target, long ptr, long memPtr);
        private delegate void EventFilterDestroyCallbackDelegate(void* target, long ptr);

        private delegate long ServiceInitCallbackDelegate(void* target, long memPtr);
        private delegate void ServiceExecuteCallbackDelegate(void* target, long svcPtr, long memPtr);
        private delegate void ServiceCancelCallbackDelegate(void* target, long svcPtr, long memPtr);
        private delegate void ServiceInvokeMethodCallbackDelegate(void* target, long svcPtr, long inMemPtr, long outMemPtr);

        private delegate int СlusterNodeFilterApplyCallbackDelegate(void* target, long memPtr);

        private delegate void NodeInfoCallbackDelegate(void* target, long memPtr);

        private delegate void OnStartCallbackDelegate(void* target, long memPtr);
        private delegate void OnStopCallbackDelegate(void* target);
        
        private delegate void ErrorCallbackDelegate(void* target, int errType, sbyte* errClsChars, int errClsCharsLen, sbyte* errMsgChars, int errMsgCharsLen, void* errData, int errDataLen);

        /// <summary>
        /// constructor.
        /// </summary>
        public UnmanagedCallbacks()
        {
            var cbs = new UnmanagedCallbackHandlers
            {
                target = IntPtr.Zero.ToPointer(), // Target is not used in .Net as we rely on dynamic FP creation.

                cacheStoreCreate = CreateFunctionPointer((CacheStoreCreateCallbackDelegate) CacheStoreCreate),
                cacheStoreInvoke = CreateFunctionPointer((CacheStoreInvokeCallbackDelegate) CacheStoreInvoke),
                cacheStoreDestroy = CreateFunctionPointer((CacheStoreDestroyCallbackDelegate) CacheStoreDestroy),

                cacheStoreSessionCreate = CreateFunctionPointer((CacheStoreSessionCreateCallbackDelegate) CacheStoreSessionCreate),
                
                cacheEntryFilterCreate = CreateFunctionPointer((CacheEntryFilterCreateCallbackDelegate)CacheEntryFilterCreate),
                cacheEntryFilterApply = CreateFunctionPointer((CacheEntryFilterApplyCallbackDelegate)CacheEntryFilterApply),
                cacheEntryFilterDestroy = CreateFunctionPointer((CacheEntryFilterDestroyCallbackDelegate)CacheEntryFilterDestroy),

                cacheInvoke = CreateFunctionPointer((CacheInvokeCallbackDelegate) CacheInvoke),

                computeTaskMap = CreateFunctionPointer((ComputeTaskMapCallbackDelegate) ComputeTaskMap),
                computeTaskJobResult =
                    CreateFunctionPointer((ComputeTaskJobResultCallbackDelegate) ComputeTaskJobResult),
                computeTaskReduce = CreateFunctionPointer((ComputeTaskReduceCallbackDelegate) ComputeTaskReduce),
                computeTaskComplete = CreateFunctionPointer((ComputeTaskCompleteCallbackDelegate) ComputeTaskComplete),
                computeJobSerialize = CreateFunctionPointer((ComputeJobSerializeCallbackDelegate) ComputeJobSerialize),
                computeJobCreate = CreateFunctionPointer((ComputeJobCreateCallbackDelegate) ComputeJobCreate),
                computeJobExecute = CreateFunctionPointer((ComputeJobExecuteCallbackDelegate) ComputeJobExecute),
                computeJobCancel = CreateFunctionPointer((ComputeJobCancelCallbackDelegate) ComputeJobCancel),
                computeJobDestroy = CreateFunctionPointer((ComputeJobDestroyCallbackDelegate) ComputeJobDestroy),
                continuousQueryListenerApply =
                    CreateFunctionPointer((ContinuousQueryListenerApplyCallbackDelegate) ContinuousQueryListenerApply),
                continuousQueryFilterCreate =
                    CreateFunctionPointer((ContinuousQueryFilterCreateCallbackDelegate) ContinuousQueryFilterCreate),
                continuousQueryFilterApply =
                    CreateFunctionPointer((ContinuousQueryFilterApplyCallbackDelegate) ContinuousQueryFilterApply),
                continuousQueryFilterRelease =
                    CreateFunctionPointer((ContinuousQueryFilterReleaseCallbackDelegate) ContinuousQueryFilterRelease),
                dataStreamerTopologyUpdate =
                    CreateFunctionPointer((DataStreamerTopologyUpdateCallbackDelegate) DataStreamerTopologyUpdate),
                dataStreamerStreamReceiverInvoke =
                    CreateFunctionPointer((DataStreamerStreamReceiverInvokeCallbackDelegate) DataStreamerStreamReceiverInvoke),
                
                futureByteResult = CreateFunctionPointer((FutureByteResultCallbackDelegate) FutureByteResult),
                futureBoolResult = CreateFunctionPointer((FutureBoolResultCallbackDelegate) FutureBoolResult),
                futureShortResult = CreateFunctionPointer((FutureShortResultCallbackDelegate) FutureShortResult),
                futureCharResult = CreateFunctionPointer((FutureCharResultCallbackDelegate) FutureCharResult),
                futureIntResult = CreateFunctionPointer((FutureIntResultCallbackDelegate) FutureIntResult),
                futureFloatResult = CreateFunctionPointer((FutureFloatResultCallbackDelegate) FutureFloatResult),
                futureLongResult = CreateFunctionPointer((FutureLongResultCallbackDelegate) FutureLongResult),
                futureDoubleResult = CreateFunctionPointer((FutureDoubleResultCallbackDelegate) FutureDoubleResult),
                futureObjectResult = CreateFunctionPointer((FutureObjectResultCallbackDelegate) FutureObjectResult),
                futureNullResult = CreateFunctionPointer((FutureNullResultCallbackDelegate) FutureNullResult),
                futureError = CreateFunctionPointer((FutureErrorCallbackDelegate) FutureError),
                lifecycleOnEvent = CreateFunctionPointer((LifecycleOnEventCallbackDelegate) LifecycleOnEvent),
                memoryReallocate = CreateFunctionPointer((MemoryReallocateCallbackDelegate) MemoryReallocate),
                nodeInfo = CreateFunctionPointer((NodeInfoCallbackDelegate) NodeInfo),
                
                messagingFilterCreate = CreateFunctionPointer((MessagingFilterCreateCallbackDelegate)MessagingFilterCreate),
                messagingFilterApply = CreateFunctionPointer((MessagingFilterApplyCallbackDelegate)MessagingFilterApply),
                messagingFilterDestroy = CreateFunctionPointer((MessagingFilterDestroyCallbackDelegate)MessagingFilterDestroy),

                eventFilterCreate = CreateFunctionPointer((EventFilterCreateCallbackDelegate)EventFilterCreate),
                eventFilterApply = CreateFunctionPointer((EventFilterApplyCallbackDelegate)EventFilterApply),
                eventFilterDestroy = CreateFunctionPointer((EventFilterDestroyCallbackDelegate)EventFilterDestroy),

                serviceInit = CreateFunctionPointer((ServiceInitCallbackDelegate)ServiceInit),
                serviceExecute = CreateFunctionPointer((ServiceExecuteCallbackDelegate)ServiceExecute),
                serviceCancel = CreateFunctionPointer((ServiceCancelCallbackDelegate)ServiceCancel),
                serviceInvokeMethod = CreateFunctionPointer((ServiceInvokeMethodCallbackDelegate)ServiceInvokeMethod),

                clusterNodeFilterApply = CreateFunctionPointer((СlusterNodeFilterApplyCallbackDelegate)СlusterNodeFilterApply),
                
                onStart = CreateFunctionPointer((OnStartCallbackDelegate)OnStart),
                onStop = CreateFunctionPointer((OnStopCallbackDelegate)OnStop),
                error = CreateFunctionPointer((ErrorCallbackDelegate)Error),
            };

            cbsPtr = Marshal.AllocHGlobal(UU.HandlersSize());

            Marshal.StructureToPtr(cbs, cbsPtr, false);

            thisHnd = GCHandle.Alloc(this);
        }

        /// <summary>
        /// Gets the handle registry.
        /// </summary>
        public HandleRegistry HandleRegistry
        {
            get { return handleRegistry; }
        }

        #region IMPLEMENTATION: CACHE

        private long CacheStoreCreate(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                var cacheStore = CacheStore.CreateInstance(memPtr, handleRegistry);

                if (grid != null)
                    cacheStore.Init(grid);
                else
                {
                    lock (initActions)
                    {
                        if (grid != null)
                            cacheStore.Init(grid);
                        else
                            initActions.Add(g => cacheStore.Init(g));
                    }
                }

                return cacheStore.Handle;
            }, true);
        }

        private int CacheStoreInvoke(void* target, long objPtr, long memPtr, void* cb)
        {
            return SafeCall(() =>
            {
                var t = handleRegistry.Get<CacheStore>(objPtr, true);

                IUnmanagedTarget cb0 = null;

                if ((long) cb != 0)
                    cb0 = new UnmanagedNonReleaseableTarget(ctx.NativeContext, cb);

                using (PlatformMemoryStream stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    return t.Invoke(stream, cb0, grid);
                }
            });
        }

        private void CacheStoreDestroy(void* target, long objPtr)
        {
            SafeCall(() => grid.HandleRegistry.Release(objPtr));
        }

        private long CacheStoreSessionCreate(void* target, long storePtr)
        {
            return SafeCall(() => grid.HandleRegistry.Allocate(new CacheStoreSession()));
        }

        private long CacheEntryFilterCreate(void* target, long memPtr)
        {
            return SafeCall(() => CacheEntryFilterHolder.CreateInstance(memPtr, grid).Handle);
        }

        private int CacheEntryFilterApply(void* target, long objPtr, long memPtr)
        {
            return SafeCall(() =>
            {
                var t = grid.HandleRegistry.Get<CacheEntryFilterHolder>(objPtr);

                using (PlatformMemoryStream stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    return t.Invoke(stream);
                }
            });
        }

        private void CacheEntryFilterDestroy(void* target, long objPtr)
        {
            SafeCall(() => grid.HandleRegistry.Release(objPtr));
        }

        private void CacheInvoke(void* target, long inMemPtr, long outMemPtr)
        {
            SafeCall(() =>
            {
                using (PlatformMemoryStream inStream = GridManager.Memory.Get(inMemPtr).Stream())
                {
                    var result = ReadAndRunCacheEntryProcessor(inStream, grid);

                    using (PlatformMemoryStream outStream = GridManager.Memory.Get(outMemPtr).Stream())
                    {
                        result.Write(outStream, grid.Marshaller);

                        outStream.SynchronizeOutput();
                    }
                }
            });
        }

        /// <summary>
        /// Reads cache entry processor and related data from stream, executes it and returns the result.
        /// </summary>
        /// <param name="inOutStream">Stream.</param>
        /// <param name="grid">Grid.</param>
        /// <returns>CacheEntryProcessor result.</returns>
        private CacheEntryProcessorResultHolder ReadAndRunCacheEntryProcessor(IPortableStream inOutStream,
            Ignite grid)
        {
            var marsh = grid.Marshaller;

            var key = marsh.Unmarshal<object>(inOutStream);
            var val = marsh.Unmarshal<object>(inOutStream);
            var isLocal = inOutStream.ReadBool();

            var holder = isLocal
                ? handleRegistry.Get<CacheEntryProcessorHolder>(inOutStream.ReadLong(), true)
                : marsh.Unmarshal<CacheEntryProcessorHolder>(inOutStream);

            return holder.Process(key, val, val != null, grid);
        }

        #endregion

        #region IMPLEMENTATION: COMPUTE

        private void ComputeTaskMap(void* target, long taskPtr, long inMemPtr, long outMemPtr)
        {
            SafeCall(() =>
            {
                using (PlatformMemoryStream inStream = GridManager.Memory.Get(inMemPtr).Stream())
                {
                    using (PlatformMemoryStream outStream = GridManager.Memory.Get(outMemPtr).Stream())
                    {
                        Task(taskPtr).Map(inStream, outStream);
                    }
                }
            });
        }

        private int ComputeTaskJobResult(void* target, long taskPtr, long jobPtr, long memPtr)
        {
            return SafeCall(() =>
            {
                var task = Task(taskPtr);

                if (memPtr == 0)
                {
                    return task.JobResultLocal(Job(jobPtr));
                }
                
                using (var stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    return task.JobResultRemote(Job(jobPtr), stream);
                }
            });
        }

        private void ComputeTaskReduce(void* target, long taskPtr)
        {
            SafeCall(() =>
            {
                var task = handleRegistry.Get<IComputeTaskHolder>(taskPtr, true);

                task.Reduce();
            });
        }

        private void ComputeTaskComplete(void* target, long taskPtr, long memPtr)
        {
            SafeCall(() =>
            {
                var task = handleRegistry.Get<IComputeTaskHolder>(taskPtr, true);

                if (memPtr == 0)
                    task.Complete(taskPtr);
                else
                {
                    using (PlatformMemoryStream stream = GridManager.Memory.Get(memPtr).Stream())
                    {
                        task.CompleteWithError(taskPtr, stream);
                    }
                }
            });
        }

        private int ComputeJobSerialize(void* target, long jobPtr, long memPtr)
        {
            return SafeCall(() =>
            {
                using (PlatformMemoryStream stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    return Job(jobPtr).Serialize(stream) ? 1 : 0;
                }
            });
        }

        private long ComputeJobCreate(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                using (PlatformMemoryStream stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    ComputeJobHolder job = ComputeJobHolder.CreateJob(grid, stream);

                    return handleRegistry.Allocate(job);
                }
            });
        }

        private void ComputeJobExecute(void* target, long jobPtr, int cancel, long memPtr)
        {
            SafeCall(() =>
            {
                var job = Job(jobPtr);

                if (memPtr == 0)
                    job.ExecuteLocal(cancel == 1);
                else
                {
                    using (PlatformMemoryStream stream = GridManager.Memory.Get(memPtr).Stream())
                    {
                        job.ExecuteRemote(stream, cancel == 1);
                    }
                }
            });
        }

        private void ComputeJobCancel(void* target, long jobPtr)
        {
            SafeCall(() =>
            {
                Job(jobPtr).Cancel();
            });
        }

        private void ComputeJobDestroy(void* target, long jobPtr)
        {
            SafeCall(() =>
            {
                handleRegistry.Release(jobPtr);
            });
        }

        /// <summary>
        /// Get compute task using it's GC handle pointer.
        /// </summary>
        /// <param name="taskPtr">Task pointer.</param>
        /// <returns>Compute task.</returns>
        private IComputeTaskHolder Task(long taskPtr)
        {
            return handleRegistry.Get<IComputeTaskHolder>(taskPtr);
        }

        /// <summary>
        /// Get comptue job using it's GC handle pointer.
        /// </summary>
        /// <param name="jobPtr">Job pointer.</param>
        /// <returns>Compute job.</returns>
        private ComputeJobHolder Job(long jobPtr)
        {
            return handleRegistry.Get<ComputeJobHolder>(jobPtr);
        }

        #endregion

        #region  IMPLEMENTATION: CONTINUOUS QUERY

        private void ContinuousQueryListenerApply(void* target, long lsnrPtr, long memPtr)
        {
            SafeCall(() =>
            {
                var hnd = handleRegistry.Get<IContinuousQueryHandleImpl>(lsnrPtr);

                hnd.Apply(GridManager.Memory.Get(memPtr).Stream());
            });
        }

        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        private long ContinuousQueryFilterCreate(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                // 1. Unmarshal filter holder.
                IPortableStream stream = GridManager.Memory.Get(memPtr).Stream();

                var reader = grid.Marshaller.StartUnmarshal(stream);

                ContinuousQueryFilterHolder filterHolder = reader.ReadObject<ContinuousQueryFilterHolder>();

                // 2. Create real filter from it's holder.
                Type filterWrapperTyp = typeof(ContinuousQueryFilter<,>)
                    .MakeGenericType(filterHolder.KeyType, filterHolder.ValueType);

                Type filterTyp = typeof(ICacheEntryEventFilter<,>)
                    .MakeGenericType(filterHolder.KeyType, filterHolder.ValueType);

                var filter = (IContinuousQueryFilter)filterWrapperTyp
                    .GetConstructor(new[] { filterTyp, typeof(bool) })
                    .Invoke(new[] { filterHolder.Filter, filterHolder.KeepPortable });

                // 3. Inject grid.
                filter.Inject(grid);

                // 4. Allocate GC handle.
                return filter.Allocate();
            });
        }

        private int ContinuousQueryFilterApply(void* target, long filterPtr, long memPtr)
        {
            return SafeCall(() =>
            {
                var holder = handleRegistry.Get<IContinuousQueryFilter>(filterPtr);

                return holder.Evaluate(GridManager.Memory.Get(memPtr).Stream()) ? 1 : 0;
            });
        }

        private void ContinuousQueryFilterRelease(void* target, long filterPtr)
        {
            SafeCall(() =>
            {
                var holder = handleRegistry.Get<IContinuousQueryFilter>(filterPtr);

                holder.Release();
            });
        }
        
        #endregion

        #region IMPLEMENTATION: DATA STREAMER

        private void DataStreamerTopologyUpdate(void* target, long ldrPtr, long topVer, int topSize)
        {
            SafeCall(() =>
            {
                var ldrRef = handleRegistry.Get<WeakReference>(ldrPtr);

                if (ldrRef == null)
                    return;

                var ldr = ldrRef.Target as IDataStreamer;

                if (ldr != null)
                    ldr.TopologyChange(topVer, topSize);
                else
                    handleRegistry.Release(ldrPtr, true);
            });
        }

        private void DataStreamerStreamReceiverInvoke(void* target, long rcvPtr, void* cache, long memPtr, 
            byte keepPortable)
        {
            SafeCall(() =>
            {
                var stream = GridManager.Memory.Get(memPtr).Stream();

                var reader = grid.Marshaller.StartUnmarshal(stream, PortableMode.FORCE_PORTABLE);

                var portableReceiver = reader.ReadObject<PortableUserObject>();

                var receiver = handleRegistry.Get<StreamReceiverHolder>(rcvPtr) ??
                    portableReceiver.Deserialize<StreamReceiverHolder>();

                if (receiver != null)
                    receiver.Receive(grid, new UnmanagedNonReleaseableTarget(ctx.NativeContext, cache), stream,
                        keepPortable != 0);
            });
        }

        #endregion
        
        #region IMPLEMENTATION: FUTURES

        private void FutureByteResult(void* target, long futPtr, int res)
        {
            SafeCall(() =>
            {
                ProcessFuture<byte>(futPtr, fut => { fut.OnResult((byte)res); });
            });
        }

        private void FutureBoolResult(void* target, long futPtr, int res)
        {
            SafeCall(() =>
            {
                ProcessFuture<bool>(futPtr, fut => { fut.OnResult(res == 1); });
            });
        }

        private void FutureShortResult(void* target, long futPtr, int res)
        {
            SafeCall(() =>
            {
                ProcessFuture<short>(futPtr, fut => { fut.OnResult((short)res); });
            });
        }

        private void FutureCharResult(void* target, long futPtr, int res)
        {
            SafeCall(() =>
            {
                ProcessFuture<char>(futPtr, fut => { fut.OnResult((char)res); });
            });
        }

        private void FutureIntResult(void* target, long futPtr, int res)
        {
            SafeCall(() =>
            {
                ProcessFuture<int>(futPtr, fut => { fut.OnResult(res); });
            });
        }

        private void FutureFloatResult(void* target, long futPtr, float res)
        {
            SafeCall(() =>
            {
                ProcessFuture<float>(futPtr, fut => { fut.OnResult(res); });
            });
        }

        private void FutureLongResult(void* target, long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<long>(futPtr, fut => { fut.OnResult(res); });
            });
        }

        private void FutureDoubleResult(void* target, long futPtr, double res)
        {
            SafeCall(() =>
            {
                ProcessFuture<double>(futPtr, fut => { fut.OnResult(res); });
            });
        }

        private void FutureObjectResult(void* target, long futPtr, long memPtr)
        {
            SafeCall(() =>
            {
                ProcessFuture(futPtr, fut =>
                {
                    IPortableStream stream = GridManager.Memory.Get(memPtr).Stream();

                    fut.OnResult(stream);
                });
            });
        }

        private void FutureNullResult(void* target, long futPtr)
        {
            SafeCall(() =>
            {
                ProcessFuture(futPtr, fut => { fut.OnNullResult(); });
            });
        }

        private void FutureError(void* target, long futPtr, long memPtr)
        {
            SafeCall(() =>
            {
                IPortableStream stream = GridManager.Memory.Get(memPtr).Stream();

                PortableReaderImpl reader = grid.Marshaller.StartUnmarshal(stream);

                string errCls = reader.ReadString();
                string errMsg = reader.ReadString();

                Exception err = ExceptionUtils.GetException(errCls, errMsg, reader);

                ProcessFuture(futPtr, fut => { fut.OnError(err); });
            });
        }

        /// <summary>
        /// Process future.
        /// </summary>
        /// <param name="futPtr">Future pointer.</param>
        /// <param name="action">Action.</param>
        private void ProcessFuture(long futPtr, Action<IFutureInternal> action)
        {
            try
            {
                action(handleRegistry.Get<IFutureInternal>(futPtr, true));
            }
            finally
            {
                handleRegistry.Release(futPtr);
            }
        }

        /// <summary>
        /// Process future.
        /// </summary>
        /// <param name="futPtr">Future pointer.</param>
        /// <param name="action">Action.</param>
        private void ProcessFuture<T>(long futPtr, Action<Future<T>> action)
        {
            try
            {
                action(handleRegistry.Get<Future<T>>(futPtr, true));
            }
            finally
            {
                handleRegistry.Release(futPtr);
            }
        }

        #endregion

        #region IMPLEMENTATION: LIFECYCLE

        private void LifecycleOnEvent(void* target, long ptr, int evt)
        {
            SafeCall(() =>
            {
                var bean = handleRegistry.Get<LifecycleBeanHolder>(ptr);

                bean.OnLifecycleEvent((LifecycleEventType)evt);
            }, true);
        }

        #endregion

        #region IMPLEMENTATION: MESSAGING

        private long MessagingFilterCreate(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                MessageFilterHolder holder = MessageFilterHolder.CreateRemote(grid, memPtr);

                return grid.HandleRegistry.AllocateSafe(holder);
            });
        }

        private int MessagingFilterApply(void* target, long ptr, long memPtr)
        {
            return SafeCall(() =>
            {
                var holder = grid.HandleRegistry.Get<MessageFilterHolder>(ptr, false);
                
                if (holder == null)
                    return 0;

                using (var stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    return holder.Invoke(stream);
                }
            });
        }

        private void MessagingFilterDestroy(void* target, long ptr)
        {
            SafeCall(() =>
            {
                grid.HandleRegistry.Release(ptr);
            });
        }
        
        #endregion

        #region IMPLEMENTATION: EVENTS

        private long EventFilterCreate(void* target, long memPtr)
        {
            return SafeCall(() => handleRegistry.Allocate(RemoteListenEventFilter.CreateInstance(memPtr, grid)));
        }

        private int EventFilterApply(void* target, long ptr, long memPtr)
        {
            return SafeCall(() =>
            {
                var holder = grid.HandleRegistry.Get<IInteropCallback>(ptr, false);

                if (holder == null)
                    return 0;

                using (var stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    return holder.Invoke(stream);
                }
            });
        }

        private void EventFilterDestroy(void* target, long ptr)
        {
            SafeCall(() =>
            {
                grid.HandleRegistry.Release(ptr);
            });
        }
        
        #endregion

        #region IMPLEMENTATION: SERVICES

        private long ServiceInit(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    var reader = grid.Marshaller.StartUnmarshal(stream);

                    bool srvKeepPortable = reader.ReadBoolean();
                    var svc = reader.ReadObject<IService>();

                    ResourceProcessor.Inject(svc, grid);

                    svc.Init(new ServiceContext(grid.Marshaller.StartUnmarshal(stream, srvKeepPortable)));

                    return handleRegistry.Allocate(svc);
                }
            });
        }

        private void ServiceExecute(void* target, long svcPtr, long memPtr)
        {
            SafeCall(() =>
            {
                var svc = handleRegistry.Get<IService>(svcPtr, true);

                using (var stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    var reader = grid.Marshaller.StartUnmarshal(stream);

                    bool srvKeepPortable = reader.ReadBoolean();

                    svc.Execute(new ServiceContext(
                        grid.Marshaller.StartUnmarshal(stream, srvKeepPortable)));
                }
            });
        }

        private void ServiceCancel(void* target, long svcPtr, long memPtr)
        {
            SafeCall(() =>
            {
                var svc = handleRegistry.Get<IService>(svcPtr, true);

                try
                {
                    using (var stream = GridManager.Memory.Get(memPtr).Stream())
                    {
                        var reader = grid.Marshaller.StartUnmarshal(stream);

                        bool srvKeepPortable = reader.ReadBoolean();

                        svc.Cancel(new ServiceContext(grid.Marshaller.StartUnmarshal(stream, srvKeepPortable)));
                    }
                }
                finally
                {
                    grid.HandleRegistry.Release(svcPtr);
                }
            });
        }

        private void ServiceInvokeMethod(void* target, long svcPtr, long inMemPtr, long outMemPtr)
        {
            SafeCall(() =>
            {
                using (var inStream = GridManager.Memory.Get(inMemPtr).Stream())
                using (var outStream = GridManager.Memory.Get(outMemPtr).Stream())
                {
                    var svc = handleRegistry.Get<IService>(svcPtr, true);

                    string mthdName;
                    object[] mthdArgs;

                    ServiceProxySerializer.ReadProxyMethod(inStream, grid.Marshaller, out mthdName, out mthdArgs);

                    var result = ServiceProxyInvoker.InvokeServiceMethod(svc, mthdName, mthdArgs);

                    ServiceProxySerializer.WriteInvocationResult(outStream, grid.Marshaller, result.Key, result.Value);

                    outStream.SynchronizeOutput();
                }
            });
        }

        private int СlusterNodeFilterApply(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = GridManager.Memory.Get(memPtr).Stream())
                {
                    var reader = grid.Marshaller.StartUnmarshal(stream);

                    var filter = (IClusterNodeFilter) reader.ReadObject<PortableOrSerializableObjectHolder>().Item;

                    return filter.Invoke(grid.GetNode(reader.ReadGuid())) ? 1 : 0;
                }
            });
        }

        #endregion

        #region IMPLEMENTATION: MISCELLANEOUS

        private void NodeInfo(void* target, long memPtr)
        {
            SafeCall(() => grid.UpdateNodeInfo(memPtr));
        }

        private void MemoryReallocate(void* target, long memPtr, int cap)
        {
            SafeCall(() =>
            {
                GridManager.Memory.Get(memPtr).Reallocate(cap);
            }, true);
        }

        private void OnStart(void* target, long memPtr)
        {
            SafeCall(() =>
            {
                Ignition.OnStart(GridManager.Memory.Get(memPtr).Stream());
            }, true);
        }

        private void OnStop(void* target)
        {
            Marshal.FreeHGlobal(cbsPtr);

            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            thisHnd.Free();

            // Allow context to be collected, which will cause resource cleanup in finalizer.
            ctx = null;
        }
        
        private void Error(void* target, int errType, sbyte* errClsChars, int errClsCharsLen, sbyte* errMsgChars,
            int errMsgCharsLen, void* errData, int errDataLen)
        {
            string errCls = U.Utf8UnmanagedToString(errClsChars, errClsCharsLen);
            string errMsg = U.Utf8UnmanagedToString(errMsgChars, errMsgCharsLen);

            switch (errType)
            {
                case ERR_GENERIC:
                    if (grid != null && errDataLen > 0)
                        throw ExceptionUtils.GetException(errCls, errMsg,
                            grid.Marshaller.StartUnmarshal(new PlatformRawMemory(errData, errDataLen).Stream()));

                    throw ExceptionUtils.GetException(errCls, errMsg);

                case ERR_JVM_INIT:
                    throw ExceptionUtils.GetJvmInitializeException(errCls, errMsg);

                case ERR_JVM_ATTACH:
                    throw new IgniteException("Failed to attach to JVM.");

                default:
                    throw new IgniteException("Unknown exception [cls=" + errCls + ", msg=" + errMsg + ']');
            }
        }

        #endregion
        
        #region HELPERS

        private void SafeCall(Action func, bool allowUnitialized = false)
        {
            if (!allowUnitialized)
                initEvent.Wait();

            try
            {
                func();
            }
            catch (Exception e)
            {
                UU.ThrowToJava(ctx.NativeContext, e);
            }
        }

        private T SafeCall<T>(Func<T> func, bool allowUnitialized = false)
        {
            if (!allowUnitialized)
                initEvent.Wait();

            try
            {
                return func();
            }
            catch (Exception e)
            {
                UU.ThrowToJava(ctx.NativeContext, e);

                return default(T);
            }
        }

        #endregion

        /// <summary>
        /// Callbacks pointer.
        /// </summary>
        public void* CallbacksPointer
        {
            get { return cbsPtr.ToPointer(); }
        }

        /// <summary>
        /// Gets the context.
        /// </summary>
        public UnmanagedContext Context
        {
            get { return ctx; }
        }

        /// <summary>
        /// Create function pointer for the given function.
        /// </summary>
        private void* CreateFunctionPointer(Delegate del)
        {
            delegates.Add(del); // Prevent delegate from being GC-ed.

            return Marshal.GetFunctionPointerForDelegate(del).ToPointer();
        }

        /// <param name="context">Context.</param>
        public void SetContext(void* context)
        {
            Debug.Assert(context != null);
            Debug.Assert(ctx == null);

            ctx = new UnmanagedContext(context);
        }

        /// <summary>
        /// Initializes this instance with grid.
        /// </summary>
        /// <param name="grid">Grid.</param>
        public void Initialize(Ignite grid)
        {
            Debug.Assert(grid != null);

            this.grid = grid;

            lock (initActions)
            {
                initActions.ForEach(x => x(grid));

                initActions.Clear();
            }

            initEvent.Set();
        }

        /// <summary>
        /// Cleanups this instance.
        /// </summary>
        public void Cleanup()
        {
            grid = null;
            
            handleRegistry.Close();
        }
    }
}
