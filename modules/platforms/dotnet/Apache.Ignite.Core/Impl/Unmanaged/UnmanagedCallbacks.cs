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
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
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
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Impl.Services;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Services;
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
        private volatile UnmanagedContext _ctx;

        /** Handle registry. */
        private readonly HandleRegistry _handleRegistry = new HandleRegistry();
        
        /** Grid. */
        private volatile Ignite _ignite;

        /** Keep references to created delegates. */
        // ReSharper disable once CollectionNeverQueried.Local
        private readonly List<Delegate> _delegates = new List<Delegate>(50);

        /** Initialized flag. */
        private readonly ManualResetEventSlim _initEvent = new ManualResetEventSlim(false);

        /** Actions to be called upon Ignite initialisation. */
        private readonly List<Action<Ignite>> _initActions = new List<Action<Ignite>>();

        /** GC handle to UnmanagedCallbacks instance to prevent it from being GCed. */
        private readonly GCHandle _thisHnd;

        /** Callbacks pointer. */
        [SuppressMessage("Microsoft.Reliability", "CA2006:UseSafeHandleToEncapsulateNativeResources")]
        private readonly IntPtr _cbsPtr;

        /** Error type: generic. */
        private const int ErrGeneric = 1;

        /** Error type: initialize. */
        private const int ErrJvmInit = 2;

        /** Error type: attach. */
        private const int ErrJvmAttach = 3;

        /** Opeartion: prepare .Net. */
        private const int OpPrepareDotNet = 1;

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

        private delegate void OnStartCallbackDelegate(void* target, void* proc, long memPtr);
        private delegate void OnStopCallbackDelegate(void* target);
        
        private delegate void ErrorCallbackDelegate(void* target, int errType, sbyte* errClsChars, int errClsCharsLen, sbyte* errMsgChars, int errMsgCharsLen, void* errData, int errDataLen);

        private delegate long ExtensionCallbackInLongOutLongDelegate(void* target, int typ, long arg1);
        private delegate long ExtensionCallbackInLongLongOutLongDelegate(void* target, int typ, long arg1, long arg2);

        private delegate void OnClientDisconnectedDelegate(void* target);
        private delegate void OnClientReconnectedDelegate(void* target, bool clusterRestarted);

        private delegate long AffinityFunctionInitDelegate(void* target, long memPtr);
        private delegate int AffinityFunctionPartitionDelegate(void* target, long ptr, long memPtr);
        private delegate void AffinityFunctionAssignPartitionsDelegate(void* target, long ptr, long inMemPtr, long outMemPtr);
        private delegate void AffinityFunctionRemoveNodeDelegate(void* target, long ptr, long memPtr);
        private delegate void AffinityFunctionDestroyDelegate(void* target, long ptr);

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
                
                extensionCbInLongOutLong = CreateFunctionPointer((ExtensionCallbackInLongOutLongDelegate)ExtensionCallbackInLongOutLong),
                extensionCbInLongLongOutLong = CreateFunctionPointer((ExtensionCallbackInLongLongOutLongDelegate)ExtensionCallbackInLongLongOutLong),

                onClientDisconnected = CreateFunctionPointer((OnClientDisconnectedDelegate)OnClientDisconnected),
                ocClientReconnected = CreateFunctionPointer((OnClientReconnectedDelegate)OnClientReconnected),

                affinityFunctionInit = CreateFunctionPointer((AffinityFunctionInitDelegate)AffinityFunctionInit),
                affinityFunctionPartition = CreateFunctionPointer((AffinityFunctionPartitionDelegate)AffinityFunctionPartition),
                affinityFunctionAssignPartitions = CreateFunctionPointer((AffinityFunctionAssignPartitionsDelegate)AffinityFunctionAssignPartitions),
                affinityFunctionRemoveNode = CreateFunctionPointer((AffinityFunctionRemoveNodeDelegate)AffinityFunctionRemoveNode),
                affinityFunctionDestroy = CreateFunctionPointer((AffinityFunctionDestroyDelegate)AffinityFunctionDestroy)
            };

            _cbsPtr = Marshal.AllocHGlobal(UU.HandlersSize());

            Marshal.StructureToPtr(cbs, _cbsPtr, false);

            _thisHnd = GCHandle.Alloc(this);
        }

        /// <summary>
        /// Gets the handle registry.
        /// </summary>
        public HandleRegistry HandleRegistry
        {
            get { return _handleRegistry; }
        }

        #region IMPLEMENTATION: CACHE

        private long CacheStoreCreate(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                var cacheStore = CacheStore.CreateInstance(memPtr, _handleRegistry);

                if (_ignite != null)
                    cacheStore.Init(_ignite);
                else
                {
                    lock (_initActions)
                    {
                        if (_ignite != null)
                            cacheStore.Init(_ignite);
                        else
                            _initActions.Add(cacheStore.Init);
                    }
                }

                return cacheStore.Handle;
            }, true);
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private int CacheStoreInvoke(void* target, long objPtr, long memPtr, void* cb)
        {
            return SafeCall(() =>
            {
                var t = _handleRegistry.Get<CacheStore>(objPtr, true);

                IUnmanagedTarget cb0 = null;

                if ((long) cb != 0)
                    cb0 = new UnmanagedNonReleaseableTarget(_ctx, cb);

                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    return t.Invoke(stream, cb0, _ignite);
                }
            });
        }

        private void CacheStoreDestroy(void* target, long objPtr)
        {
            SafeCall(() => _ignite.HandleRegistry.Release(objPtr));
        }

        private long CacheStoreSessionCreate(void* target, long storePtr)
        {
            return SafeCall(() => _ignite.HandleRegistry.Allocate(new CacheStoreSession()));
        }

        private long CacheEntryFilterCreate(void* target, long memPtr)
        {
            return SafeCall(() => _handleRegistry.Allocate(CacheEntryFilterHolder.CreateInstance(memPtr, _ignite)));
        }

        private int CacheEntryFilterApply(void* target, long objPtr, long memPtr)
        {
            return SafeCall(() =>
            {
                var t = _ignite.HandleRegistry.Get<CacheEntryFilterHolder>(objPtr);

                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    return t.Invoke(stream);
                }
            });
        }

        private void CacheEntryFilterDestroy(void* target, long objPtr)
        {
            SafeCall(() => _ignite.HandleRegistry.Release(objPtr));
        }

        private void CacheInvoke(void* target, long inMemPtr, long outMemPtr)
        {
            SafeCall(() =>
            {
                using (PlatformMemoryStream inStream = IgniteManager.Memory.Get(inMemPtr).GetStream())
                {
                    var result = ReadAndRunCacheEntryProcessor(inStream, _ignite);

                    using (PlatformMemoryStream outStream = IgniteManager.Memory.Get(outMemPtr).GetStream())
                    {
                        result.Write(outStream, _ignite.Marshaller);

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
        private CacheEntryProcessorResultHolder ReadAndRunCacheEntryProcessor(IBinaryStream inOutStream,
            Ignite grid)
        {
            var marsh = grid.Marshaller;

            var key = marsh.Unmarshal<object>(inOutStream);
            var val = marsh.Unmarshal<object>(inOutStream);
            var isLocal = inOutStream.ReadBool();

            var holder = isLocal
                ? _handleRegistry.Get<CacheEntryProcessorHolder>(inOutStream.ReadLong(), true)
                : marsh.Unmarshal<CacheEntryProcessorHolder>(inOutStream);

            return holder.Process(key, val, val != null, grid);
        }

        #endregion

        #region IMPLEMENTATION: COMPUTE

        private void ComputeTaskMap(void* target, long taskPtr, long inMemPtr, long outMemPtr)
        {
            SafeCall(() =>
            {
                using (PlatformMemoryStream inStream = IgniteManager.Memory.Get(inMemPtr).GetStream())
                {
                    using (PlatformMemoryStream outStream = IgniteManager.Memory.Get(outMemPtr).GetStream())
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
                
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    return task.JobResultRemote(Job(jobPtr), stream);
                }
            });
        }

        private void ComputeTaskReduce(void* target, long taskPtr)
        {
            SafeCall(() =>
            {
                var task = _handleRegistry.Get<IComputeTaskHolder>(taskPtr, true);

                task.Reduce();
            });
        }

        private void ComputeTaskComplete(void* target, long taskPtr, long memPtr)
        {
            SafeCall(() =>
            {
                var task = _handleRegistry.Get<IComputeTaskHolder>(taskPtr, true);

                if (memPtr == 0)
                    task.Complete(taskPtr);
                else
                {
                    using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
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
                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    return Job(jobPtr).Serialize(stream) ? 1 : 0;
                }
            });
        }

        private long ComputeJobCreate(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    ComputeJobHolder job = ComputeJobHolder.CreateJob(_ignite, stream);

                    return _handleRegistry.Allocate(job);
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
                    using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
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
                _handleRegistry.Release(jobPtr);
            });
        }

        /// <summary>
        /// Get compute task using it's GC handle pointer.
        /// </summary>
        /// <param name="taskPtr">Task pointer.</param>
        /// <returns>Compute task.</returns>
        private IComputeTaskHolder Task(long taskPtr)
        {
            return _handleRegistry.Get<IComputeTaskHolder>(taskPtr);
        }

        /// <summary>
        /// Get comptue job using it's GC handle pointer.
        /// </summary>
        /// <param name="jobPtr">Job pointer.</param>
        /// <returns>Compute job.</returns>
        private ComputeJobHolder Job(long jobPtr)
        {
            return _handleRegistry.Get<ComputeJobHolder>(jobPtr);
        }

        #endregion

        #region  IMPLEMENTATION: CONTINUOUS QUERY

        private void ContinuousQueryListenerApply(void* target, long lsnrPtr, long memPtr)
        {
            SafeCall(() =>
            {
                var hnd = _handleRegistry.Get<IContinuousQueryHandleImpl>(lsnrPtr);

                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    hnd.Apply(stream);
                }
            });
        }

        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        private long ContinuousQueryFilterCreate(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                // 1. Unmarshal filter holder.
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    var filterHolder = reader.ReadObject<ContinuousQueryFilterHolder>();

                    // 2. Create real filter from it's holder.
                    var filter = (IContinuousQueryFilter) DelegateTypeDescriptor.GetContinuousQueryFilterCtor(
                        filterHolder.Filter.GetType())(filterHolder.Filter, filterHolder.KeepBinary);

                    // 3. Inject grid.
                    filter.Inject(_ignite);

                    // 4. Allocate GC handle.
                    return filter.Allocate();
                }
            });
        }

        private int ContinuousQueryFilterApply(void* target, long filterPtr, long memPtr)
        {
            return SafeCall(() =>
            {
                var holder = _handleRegistry.Get<IContinuousQueryFilter>(filterPtr);

                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    return holder.Evaluate(stream) ? 1 : 0;
                }
            });
        }

        private void ContinuousQueryFilterRelease(void* target, long filterPtr)
        {
            SafeCall(() =>
            {
                var holder = _handleRegistry.Get<IContinuousQueryFilter>(filterPtr);

                holder.Release();
            });
        }
        
        #endregion

        #region IMPLEMENTATION: DATA STREAMER

        private void DataStreamerTopologyUpdate(void* target, long ldrPtr, long topVer, int topSize)
        {
            SafeCall(() =>
            {
                var ldrRef = _handleRegistry.Get<WeakReference>(ldrPtr);

                if (ldrRef == null)
                    return;

                var ldr = ldrRef.Target as IDataStreamer;

                if (ldr != null)
                    ldr.TopologyChange(topVer, topSize);
                else
                    _handleRegistry.Release(ldrPtr, true);
            });
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private void DataStreamerStreamReceiverInvoke(void* target, long rcvPtr, void* cache, long memPtr, 
            byte keepBinary)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream, BinaryMode.ForceBinary);

                    var binaryReceiver = reader.ReadObject<BinaryObject>();

                    var receiver = _handleRegistry.Get<StreamReceiverHolder>(rcvPtr) ??
                                   binaryReceiver.Deserialize<StreamReceiverHolder>();

                    if (receiver != null)
                        receiver.Receive(_ignite, new UnmanagedNonReleaseableTarget(_ctx, cache), stream,
                            keepBinary != 0);
                }
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
                    using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                    {
                        fut.OnResult(stream);
                    }
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
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    string errCls = reader.ReadString();
                    string errMsg = reader.ReadString();

                    Exception err = ExceptionUtils.GetException(_ignite, errCls, errMsg, reader);

                    ProcessFuture(futPtr, fut => { fut.OnError(err); });
                }
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
                action(_handleRegistry.Get<IFutureInternal>(futPtr, true));
            }
            finally
            {
                _handleRegistry.Release(futPtr);
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
                action(_handleRegistry.Get<Future<T>>(futPtr, true));
            }
            finally
            {
                _handleRegistry.Release(futPtr);
            }
        }

        #endregion

        #region IMPLEMENTATION: LIFECYCLE

        private void LifecycleOnEvent(void* target, long ptr, int evt)
        {
            SafeCall(() =>
            {
                var bean = _handleRegistry.Get<LifecycleBeanHolder>(ptr);

                bean.OnLifecycleEvent((LifecycleEventType)evt);
            }, true);
        }

        #endregion

        #region IMPLEMENTATION: MESSAGING

        private long MessagingFilterCreate(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                MessageListenerHolder holder = MessageListenerHolder.CreateRemote(_ignite, memPtr);

                return _ignite.HandleRegistry.AllocateSafe(holder);
            });
        }

        private int MessagingFilterApply(void* target, long ptr, long memPtr)
        {
            return SafeCall(() =>
            {
                var holder = _ignite.HandleRegistry.Get<MessageListenerHolder>(ptr, false);
                
                if (holder == null)
                    return 0;

                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    return holder.Invoke(stream);
                }
            });
        }

        private void MessagingFilterDestroy(void* target, long ptr)
        {
            SafeCall(() =>
            {
                _ignite.HandleRegistry.Release(ptr);
            });
        }
        
        #endregion

        #region IMPLEMENTATION: EXTENSIONS

        private long ExtensionCallbackInLongOutLong(void* target, int op, long arg1)
        {
            throw new InvalidOperationException("Unsupported operation type: " + op);
        }

        private long ExtensionCallbackInLongLongOutLong(void* target, int op, long arg1, long arg2)
        {
            return SafeCall(() =>
            {
                switch (op)
                {
                    case OpPrepareDotNet:
                        using (var inStream = IgniteManager.Memory.Get(arg1).GetStream())
                        using (var outStream = IgniteManager.Memory.Get(arg2).GetStream())
                        {
                            Ignition.OnPrepare(inStream, outStream, _handleRegistry);

                            return 0;
                        }

                    default:
                        throw new InvalidOperationException("Unsupported operation type: " + op);
                }
            }, op == OpPrepareDotNet);
        }

        #endregion

        #region IMPLEMENTATION: EVENTS

        private long EventFilterCreate(void* target, long memPtr)
        {
            return SafeCall(() => _handleRegistry.Allocate(RemoteListenEventFilter.CreateInstance(memPtr, _ignite)));
        }

        private int EventFilterApply(void* target, long ptr, long memPtr)
        {
            return SafeCall(() =>
            {
                var holder = _ignite.HandleRegistry.Get<IInteropCallback>(ptr, false);

                if (holder == null)
                    return 0;

                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    return holder.Invoke(stream);
                }
            });
        }

        private void EventFilterDestroy(void* target, long ptr)
        {
            SafeCall(() =>
            {
                _ignite.HandleRegistry.Release(ptr);
            });
        }
        
        #endregion

        #region IMPLEMENTATION: SERVICES

        private long ServiceInit(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    bool srvKeepBinary = reader.ReadBoolean();
                    var svc = reader.ReadObject<IService>();

                    ResourceProcessor.Inject(svc, _ignite);

                    svc.Init(new ServiceContext(_ignite.Marshaller.StartUnmarshal(stream, srvKeepBinary)));

                    return _handleRegistry.Allocate(svc);
                }
            });
        }

        private void ServiceExecute(void* target, long svcPtr, long memPtr)
        {
            SafeCall(() =>
            {
                var svc = _handleRegistry.Get<IService>(svcPtr);

                // Ignite does not guarantee that Cancel is called after Execute exits
                // So missing handle is a valid situation
                if (svc == null)
                    return;   

                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    bool srvKeepBinary = reader.ReadBoolean();

                    svc.Execute(new ServiceContext(
                        _ignite.Marshaller.StartUnmarshal(stream, srvKeepBinary)));
                }
            });
        }

        private void ServiceCancel(void* target, long svcPtr, long memPtr)
        {
            SafeCall(() =>
            {
                var svc = _handleRegistry.Get<IService>(svcPtr, true);

                try
                {
                    using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                    {
                        var reader = _ignite.Marshaller.StartUnmarshal(stream);

                        bool srvKeepBinary = reader.ReadBoolean();

                        svc.Cancel(new ServiceContext(_ignite.Marshaller.StartUnmarshal(stream, srvKeepBinary)));
                    }
                }
                finally
                {
                    _ignite.HandleRegistry.Release(svcPtr);
                }
            });
        }

        private void ServiceInvokeMethod(void* target, long svcPtr, long inMemPtr, long outMemPtr)
        {
            SafeCall(() =>
            {
                using (var inStream = IgniteManager.Memory.Get(inMemPtr).GetStream())
                using (var outStream = IgniteManager.Memory.Get(outMemPtr).GetStream())
                {
                    var svc = _handleRegistry.Get<IService>(svcPtr, true);

                    string mthdName;
                    object[] mthdArgs;

                    ServiceProxySerializer.ReadProxyMethod(inStream, _ignite.Marshaller, out mthdName, out mthdArgs);

                    var result = ServiceProxyInvoker.InvokeServiceMethod(svc, mthdName, mthdArgs);

                    ServiceProxySerializer.WriteInvocationResult(outStream, _ignite.Marshaller, result.Key, result.Value);

                    outStream.SynchronizeOutput();
                }
            });
        }

        private int СlusterNodeFilterApply(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    var filter = reader.ReadObject<IClusterNodeFilter>();

                    return filter.Invoke(_ignite.GetNode(reader.ReadGuid())) ? 1 : 0;
                }
            });
        }

        #endregion

        #region IMPLEMENTATION: MISCELLANEOUS

        private void NodeInfo(void* target, long memPtr)
        {
            SafeCall(() => _ignite.UpdateNodeInfo(memPtr));
        }

        private void MemoryReallocate(void* target, long memPtr, int cap)
        {
            SafeCall(() =>
            {
                IgniteManager.Memory.Get(memPtr).Reallocate(cap);
            }, true);
        }

        private void OnStart(void* target, void* proc, long memPtr)
        {
            SafeCall(() =>
            {
                var proc0 = UnmanagedUtils.Acquire(_ctx, proc);

                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    Ignition.OnStart(proc0, stream);
                }
            }, true);
        }

        private void OnStop(void* target)
        {
            Marshal.FreeHGlobal(_cbsPtr);

            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            _thisHnd.Free();

            // Allow context to be collected, which will cause resource cleanup in finalizer.
            _ctx = null;

            // Notify grid
            var ignite = _ignite;

            if (ignite != null)
                ignite.AfterNodeStop();
        }
        
        private void Error(void* target, int errType, sbyte* errClsChars, int errClsCharsLen, sbyte* errMsgChars,
            int errMsgCharsLen, void* errData, int errDataLen)
        {
            string errCls = IgniteUtils.Utf8UnmanagedToString(errClsChars, errClsCharsLen);
            string errMsg = IgniteUtils.Utf8UnmanagedToString(errMsgChars, errMsgCharsLen);

            switch (errType)
            {
                case ErrGeneric:
                    if (_ignite != null && errDataLen > 0)
                    {
                        // Stream disposal intentionally omitted: IGNITE-1598
                        var stream = new PlatformRawMemory(errData, errDataLen).GetStream();

                        throw ExceptionUtils.GetException(_ignite, errCls, errMsg, _ignite.Marshaller.StartUnmarshal(stream));
                    }

                    throw ExceptionUtils.GetException(_ignite, errCls, errMsg);

                case ErrJvmInit:
                    throw ExceptionUtils.GetJvmInitializeException(errCls, errMsg);

                case ErrJvmAttach:
                    throw new IgniteException("Failed to attach to JVM.");

                default:
                    throw new IgniteException("Unknown exception [cls=" + errCls + ", msg=" + errMsg + ']');
            }
        }

        private void OnClientDisconnected(void* target)
        {
            SafeCall(() =>
            {
                _ignite.OnClientDisconnected();
            });
        }

        private void OnClientReconnected(void* target, bool clusterRestarted)
        {
            SafeCall(() =>
            {
                _ignite.OnClientReconnected(clusterRestarted);
            });
        }

        #endregion

        #region AffinityFunction

        private long AffinityFunctionInit(void* target, long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    var funcOrTypeName = reader.ReadObject<object>();

                    var func = funcOrTypeName as IAffinityFunction
                               ?? IgniteUtils.CreateInstance<IAffinityFunction>((string) funcOrTypeName,
                                   reader.ReadDictionaryAsGeneric<string, object>());

                    ResourceProcessor.Inject(func, _ignite);

                    return _handleRegistry.Allocate(func);
                }
            });
        }

        private int AffinityFunctionPartition(void* target, long ptr, long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var key = _ignite.Marshaller.Unmarshal<object>(stream);

                    return _handleRegistry.Get<IAffinityFunction>(ptr, true).GetPartition(key);
                }
            });
        }

        private void AffinityFunctionAssignPartitions(void* target, long ptr, long inMemPtr, long outMemPtr)
        {
            SafeCall(() =>
            {
                using (var inStream = IgniteManager.Memory.Get(inMemPtr).GetStream())
                {
                    var ctx = new AffinityFunctionContext(_ignite.Marshaller.StartUnmarshal(inStream));
                    var func = _handleRegistry.Get<IAffinityFunction>(ptr, true);
                    var parts = func.AssignPartitions(ctx);

                    if (parts == null)
                        throw new IgniteException(func.GetType() + ".AssignPartitions() returned invalid result: null");

                    using (var outStream = IgniteManager.Memory.Get(outMemPtr).GetStream())
                    {
                        var writer = _ignite.Marshaller.StartMarshal(outStream);

                        var partCnt = 0;
                        writer.WriteInt(partCnt);  // reserve size

                        foreach (var part in parts)
                        {
                            if (part == null)
                                throw new IgniteException(func.GetType() +
                                                          ".AssignPartitions() returned invalid partition: null");

                            partCnt++;

                            var nodeCnt = 0;
                            var cntPos = outStream.Position;
                            writer.WriteInt(nodeCnt);  // reserve size

                            foreach (var node in part)
                            {
                                nodeCnt++;
                                writer.WriteGuid(node.Id);
                            }

                            var endPos = outStream.Position;
                            outStream.Seek(cntPos, SeekOrigin.Begin);
                            outStream.WriteInt(nodeCnt);
                            outStream.Seek(endPos, SeekOrigin.Begin);
                        }

                        outStream.SynchronizeOutput();
                        outStream.Seek(0, SeekOrigin.Begin);
                        writer.WriteInt(partCnt);
                    }
                }
            });
        }

        private void AffinityFunctionRemoveNode(void* target, long ptr, long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var nodeId = _ignite.Marshaller.Unmarshal<Guid>(stream);

                    _handleRegistry.Get<IAffinityFunction>(ptr, true).RemoveNode(nodeId);
                }
            });
        }

        private void AffinityFunctionDestroy(void* target, long ptr)
        {
            SafeCall(() =>
            {
                _handleRegistry.Release(ptr);
            });
        }

        #endregion

        #region HELPERS

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void SafeCall(Action func, bool allowUnitialized = false)
        {
            if (!allowUnitialized)
                _initEvent.Wait();

            try
            {
                func();
            }
            catch (Exception e)
            {
                UU.ThrowToJava(_ctx.NativeContext, e);
            }
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private T SafeCall<T>(Func<T> func, bool allowUnitialized = false)
        {
            if (!allowUnitialized)
                _initEvent.Wait();

            try
            {
                return func();
            }
            catch (Exception e)
            {
                UU.ThrowToJava(_ctx.NativeContext, e);

                return default(T);
            }
        }

        #endregion

        /// <summary>
        /// Callbacks pointer.
        /// </summary>
        public void* CallbacksPointer
        {
            get { return _cbsPtr.ToPointer(); }
        }

        /// <summary>
        /// Gets the context.
        /// </summary>
        public UnmanagedContext Context
        {
            get { return _ctx; }
        }

        /// <summary>
        /// Create function pointer for the given function.
        /// </summary>
        private void* CreateFunctionPointer(Delegate del)
        {
            _delegates.Add(del); // Prevent delegate from being GC-ed.

            return Marshal.GetFunctionPointerForDelegate(del).ToPointer();
        }

        /// <param name="context">Context.</param>
        public void SetContext(void* context)
        {
            Debug.Assert(context != null);
            Debug.Assert(_ctx == null);

            _ctx = new UnmanagedContext(context);
        }

        /// <summary>
        /// Initializes this instance with grid.
        /// </summary>
        /// <param name="grid">Grid.</param>
        public void Initialize(Ignite grid)
        {
            Debug.Assert(grid != null);

            _ignite = grid;

            lock (_initActions)
            {
                _initActions.ForEach(x => x(grid));

                _initActions.Clear();
            }

            _initEvent.Set();
        }

        /// <summary>
        /// Cleanups this instance.
        /// </summary>
        public void Cleanup()
        {
            _ignite = null;
            
            _handleRegistry.Close();
        }
    }
}
