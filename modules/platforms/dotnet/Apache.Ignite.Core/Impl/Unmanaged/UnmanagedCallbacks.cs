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
    using System.Globalization;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Cache.Store;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Compute;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Events;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Log;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Messaging;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Impl.Services;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
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
        /** Console write delegate. */
        private static readonly ConsoleWriteDelegate ConsoleWriteDel = ConsoleWrite;

        /** Console write pointer. */
        private static readonly void* ConsoleWritePtr =
            Marshal.GetFunctionPointerForDelegate(ConsoleWriteDel).ToPointer();

        /** Unmanaged context. */
        private volatile UnmanagedContext _ctx;

        /** Handle registry. */
        private readonly HandleRegistry _handleRegistry = new HandleRegistry();

        /** Grid. */
        private volatile Ignite _ignite;

        /** Keep references to created delegates. */
        // ReSharper disable once CollectionNeverQueried.Local
        private readonly List<Delegate> _delegates = new List<Delegate>(6);

        /** Initialized flag. */
        private readonly ManualResetEventSlim _initEvent = new ManualResetEventSlim(false);

        /** Actions to be called upon Ignite initialization. */
        private readonly List<Action<Ignite>> _initActions = new List<Action<Ignite>>();

        /** GC handle to UnmanagedCallbacks instance to prevent it from being GCed. */
        private readonly GCHandle _thisHnd;

        /** Callbacks pointer. */
        [SuppressMessage("Microsoft.Reliability", "CA2006:UseSafeHandleToEncapsulateNativeResources")]
        private readonly IntPtr _cbsPtr;

        /** Log. */
        private readonly ILogger _log;

        /** Error type: generic. */
        private const int ErrGeneric = 1;

        /** Error type: initialize. */
        private const int ErrJvmInit = 2;

        /** Error type: attach. */
        private const int ErrJvmAttach = 3;

        /** Operation: prepare .Net. */
        private const int OpPrepareDotNet = 1;

        private delegate void ErrorCallbackDelegate(void* target, int errType, sbyte* errClsChars, int errClsCharsLen, sbyte* errMsgChars, int errMsgCharsLen, sbyte* stackTraceChars, int stackTraceCharsLen, void* errData, int errDataLen);

        private delegate void LoggerLogDelegate(void* target, int level, sbyte* messageChars, int messageCharsLen, sbyte* categoryChars, int categoryCharsLen, sbyte* errorInfoChars, int errorInfoCharsLen, long memPtr);
        private delegate bool LoggerIsLevelEnabledDelegate(void* target, int level);

        private delegate void ConsoleWriteDelegate(sbyte* chars, int charsLen, bool isErr);

        private delegate long InLongOutLongDelegate(void* target, int type, long val);
        private delegate long InLongLongLongObjectOutLongDelegate(void* target, int type, long val1, long val2, long val3, void* arg);

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="log">Logger.</param>
        public UnmanagedCallbacks(ILogger log)
        {
            Debug.Assert(log != null);
            _log = log;

            var cbs = new UnmanagedCallbackHandlers
            {
                target = IntPtr.Zero.ToPointer(), // Target is not used in .Net as we rely on dynamic FP creation.

                error = CreateFunctionPointer((ErrorCallbackDelegate)Error),

                loggerLog = CreateFunctionPointer((LoggerLogDelegate)LoggerLog),
                loggerIsLevelEnabled = CreateFunctionPointer((LoggerIsLevelEnabledDelegate)LoggerIsLevelEnabled),

                inLongOutLong = CreateFunctionPointer((InLongOutLongDelegate)InLongOutLong),
                inLongLongObjectOutLong = CreateFunctionPointer((InLongLongLongObjectOutLongDelegate)InLongLongLongObjectOutLong)
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

        #region IMPLEMENTATION: GENERAL PURPOSE

        private long InLongOutLong(void* target, int type, long val)
        {
            // TODO: Wrap all in a SafeCall? What to do with gateway?
            // TODO: Each component registers itself for a callback with a "allowUninit" flag?
            // TODO: Replace switch with array. Each entry is a delegate + allowUninit flag.
            var op = (UnmanagedCallbackOp) type;

            switch (op)
            {
                case UnmanagedCallbackOp.CacheStoreCreate:
                    return CacheStoreCreate(val);

                case UnmanagedCallbackOp.CacheStoreInvoke:
                    return CacheStoreInvoke(val);

                case UnmanagedCallbackOp.CacheStoreDestroy:
                    CacheStoreDestroy(val);
                    return 0;

                case UnmanagedCallbackOp.CacheStoreSessionCreate:
                    return CacheStoreSessionCreate();

                case UnmanagedCallbackOp.CacheEntryFilterCreate:
                    return CacheEntryFilterCreate(val);

                case UnmanagedCallbackOp.CacheEntryFilterApply:
                    return CacheEntryFilterApply(val);

                case UnmanagedCallbackOp.CacheEntryFilterDestroy:
                    CacheEntryFilterDestroy(val);
                    return 0;

                case UnmanagedCallbackOp.CacheInvoke:
                    CacheInvoke(val);
                    return 0;

                case UnmanagedCallbackOp.ComputeTaskMap:
                    ComputeTaskMap(val);
                    return 0;

                case UnmanagedCallbackOp.ComputeTaskJobResult:
                    return ComputeTaskJobResult(val);

                case UnmanagedCallbackOp.ComputeTaskReduce:
                    ComputeTaskReduce(val);
                    return 0;

                case UnmanagedCallbackOp.ComputeJobCreate:
                    return ComputeJobCreate(val);

                case UnmanagedCallbackOp.ComputeJobExecute:
                    ComputeJobExecute(val);
                    return 0;

                case UnmanagedCallbackOp.ComputeJobCancel:
                    ComputeJobCancel(val);
                    return 0;

                case UnmanagedCallbackOp.ComputeJobDestroy:
                    ComputeJobDestroy(val);
                    return 0;

                case UnmanagedCallbackOp.ContinuousQueryListenerApply:
                    ContinuousQueryListenerApply(val);
                    return 0;

                case UnmanagedCallbackOp.ContinuousQueryFilterCreate:
                    return ContinuousQueryFilterCreate(val);

                case UnmanagedCallbackOp.ContinuousQueryFilterApply:
                    return ContinuousQueryFilterApply(val);

                case UnmanagedCallbackOp.ContinuousQueryFilterRelease:
                    ContinuousQueryFilterRelease(val);
                    return 0;

                case UnmanagedCallbackOp.NodeInfo:
                    NodeInfo(val);
                    return 0;

                case UnmanagedCallbackOp.MessagingFilterCreate:
                    return MessagingFilterCreate(val);

                case UnmanagedCallbackOp.MessagingFilterDestroy:
                    MessagingFilterDestroy(val);
                    return 0;

                case UnmanagedCallbackOp.EventFilterCreate:
                    return EventFilterCreate(val);

                case UnmanagedCallbackOp.EventFilterDestroy:
                    EventFilterDestroy(val);
                    return 0;

                case UnmanagedCallbackOp.ServiceInit:
                    return ServiceInit(val);

                case UnmanagedCallbackOp.ServiceExecute:
                    ServiceExecute(val);
                    return 0;

                case UnmanagedCallbackOp.ServiceCancel:
                    ServiceCancel(val);
                    return 0;

                case UnmanagedCallbackOp.ServiceInvokeMethod:
                    ServiceInvokeMethod(val);
                    return 0;

                case UnmanagedCallbackOp.ClusterNodeFilterApply:
                    return ClusterNodeFilterApply(val);

                case UnmanagedCallbackOp.OnStop:
                    OnStop();
                    return 0;

                case UnmanagedCallbackOp.OnClientDisconnected:
                    OnClientDisconnected();
                    return 0;

                case UnmanagedCallbackOp.OnClientReconnected:
                    OnClientReconnected(val != 0);
                    return 0;

                case UnmanagedCallbackOp.AffinityFunctionPartition:
                    return AffinityFunctionPartition(val);

                case UnmanagedCallbackOp.AffinityFunctionAssignPartitions:
                    AffinityFunctionAssignPartitions(val);
                    return 0;

                case UnmanagedCallbackOp.AffinityFunctionRemoveNode:
                    AffinityFunctionRemoveNode(val);
                    return 0;

                case UnmanagedCallbackOp.AffinityFunctionDestroy:
                    AffinityFunctionDestroy(val);
                    return 0;

                default:
                    throw new InvalidOperationException("Invalid callback code: " + type);
            }
        }

        private long InLongLongLongObjectOutLong(void* target, int type, long val1, long val2, long val3, void* arg)
        {
            var op = (UnmanagedCallbackOp)type;

            switch (op)
            {
                case UnmanagedCallbackOp.AffinityFunctionInit:
                    return AffinityFunctionInit(val1, arg);

                case UnmanagedCallbackOp.ComputeTaskLocalJobResult:
                    return ComputeTaskLocalJobResult(val1, val2);

                case UnmanagedCallbackOp.ComputeTaskComplete:
                    ComputeTaskComplete(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.ComputeJobSerialize:
                    return ComputeJobSerialize(val1, val2);

                case UnmanagedCallbackOp.ComputeJobExecuteLocal:
                    ComputeJobExecuteLocal(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.DataStreamerTopologyUpdate:
                    DataStreamerTopologyUpdate(val1, val2, (int) val3);
                    return 0;

                case UnmanagedCallbackOp.DataStreamerStreamReceiverInvoke:
                    DataStreamerStreamReceiverInvoke(arg, val1);
                    return 0;

                case UnmanagedCallbackOp.FutureByteResult:
                    FutureByteResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureBoolResult:
                    FutureBoolResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureShortResult:
                    FutureShortResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureCharResult:
                    FutureCharResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureIntResult:
                    FutureIntResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureFloatResult:
                    FutureFloatResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureLongResult:
                    FutureLongResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureDoubleResult:
                    FutureDoubleResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureObjectResult:
                    FutureObjectResult(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.FutureNullResult:
                    FutureNullResult(val1);
                    return 0;

                case UnmanagedCallbackOp.FutureError:
                    FutureError(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.LifecycleOnEvent:
                    LifecycleOnEvent(val1, val2);
                    return 0;

                case UnmanagedCallbackOp.MemoryReallocate:
                    MemoryReallocate(val1, (int) val2);
                    return 0;

                case UnmanagedCallbackOp.MessagingFilterApply:
                    return MessagingFilterApply(val1, val2);

                case UnmanagedCallbackOp.EventFilterApply:
                    return EventFilterApply(val1, val2);

                case UnmanagedCallbackOp.OnStart:
                    OnStart(arg, val1);
                    return 0;

                case UnmanagedCallbackOp.ExtensionInLongOutLong:
                    return ExtensionCallbackInLongOutLong((int) val1);

                case UnmanagedCallbackOp.ExtensionInLongLongOutLong:
                    return ExtensionCallbackInLongLongOutLong((int) val1, val2, val3);

                default:
                    throw new InvalidOperationException("Invalid callback code: " + op);
            }
        }

        #endregion

        #region IMPLEMENTATION: CACHE

        private long CacheStoreCreate(long memPtr)
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

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private int CacheStoreInvoke(long memPtr)
        {
            return SafeCall(() =>
            {
                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    try
                    {
                        var store = _handleRegistry.Get<CacheStore>(stream.ReadLong(), true);

                        return store.Invoke(stream, _ignite);
                    }
                    catch (Exception e)
                    {
                        stream.Reset();

                        _ignite.Marshaller.StartMarshal(stream).WriteObject(e);

                        return -1;
                    }
                }
            });
        }

        private void CacheStoreDestroy(long objPtr)
        {
            SafeCall(() => _ignite.HandleRegistry.Release(objPtr));
        }

        private long CacheStoreSessionCreate()
        {
            return SafeCall(() => _ignite.HandleRegistry.Allocate(new CacheStoreSession()));
        }

        private long CacheEntryFilterCreate(long memPtr)
        {
            return SafeCall(() => _handleRegistry.Allocate(CacheEntryFilterHolder.CreateInstance(memPtr, _ignite)));
        }

        private int CacheEntryFilterApply(long memPtr)
        {
            return SafeCall(() =>
            {
                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var t = _ignite.HandleRegistry.Get<CacheEntryFilterHolder>(stream.ReadLong());

                    return t.Invoke(stream);
                }
            });
        }

        private void CacheEntryFilterDestroy(long objPtr)
        {
            SafeCall(() => _ignite.HandleRegistry.Release(objPtr));
        }

        private void CacheInvoke(long memPtr)
        {
            SafeCall(() =>
            {
                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var result = ReadAndRunCacheEntryProcessor(stream, _ignite);

                    stream.Reset();

                    result.Write(stream, _ignite.Marshaller);

                    stream.SynchronizeOutput();
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

        private void ComputeTaskMap(long memPtr)
        {
            SafeCall(() =>
            {
                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    Task(stream.ReadLong()).Map(stream);
                }
            });
        }

        private int ComputeTaskLocalJobResult(long taskPtr, long jobPtr)
        {
            return SafeCall(() =>
            {
                var task = Task(taskPtr);

                return task.JobResultLocal(Job(jobPtr));
            });
        }

        private int ComputeTaskJobResult(long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var task = Task(stream.ReadLong());

                    var job = Job(stream.ReadLong());

                    return task.JobResultRemote(job, stream);
                }
            });
        }

        private void ComputeTaskReduce(long taskPtr)
        {
            SafeCall(() =>
            {
                var task = _handleRegistry.Get<IComputeTaskHolder>(taskPtr, true);

                task.Reduce();
            });
        }

        private void ComputeTaskComplete(long taskPtr, long memPtr)
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

        private int ComputeJobSerialize(long jobPtr, long memPtr)
        {
            return SafeCall(() =>
            {
                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    return Job(jobPtr).Serialize(stream) ? 1 : 0;
                }
            });
        }

        private long ComputeJobCreate(long memPtr)
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

        private void ComputeJobExecuteLocal(long jobPtr, long cancel)
        {
            SafeCall(() =>
            {
                var job = Job(jobPtr);

                job.ExecuteLocal(cancel == 1);
            });
        }

        private void ComputeJobExecute(long memPtr)
        {
            SafeCall(() =>
            {
                using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var job = Job(stream.ReadLong());

                    var cancel = stream.ReadBool();

                    stream.Reset();

                    job.ExecuteRemote(stream, cancel);
                }
            });
        }

        private void ComputeJobCancel(long jobPtr)
        {
            SafeCall(() =>
            {
                Job(jobPtr).Cancel();
            });
        }

        private void ComputeJobDestroy(long jobPtr)
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
        /// Get compute job using it's GC handle pointer.
        /// </summary>
        /// <param name="jobPtr">Job pointer.</param>
        /// <returns>Compute job.</returns>
        private ComputeJobHolder Job(long jobPtr)
        {
            return _handleRegistry.Get<ComputeJobHolder>(jobPtr);
        }

        #endregion

        #region  IMPLEMENTATION: CONTINUOUS QUERY

        private void ContinuousQueryListenerApply(long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var hnd = _handleRegistry.Get<IContinuousQueryHandleImpl>(stream.ReadLong());

                    hnd.Apply(stream);
                }
            });
        }

        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        private long ContinuousQueryFilterCreate(long memPtr)
        {
            return SafeCall(() =>
            {
                // 1. Unmarshal filter holder.
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    var filterHolder = reader.ReadObject<ContinuousQueryFilterHolder>();

                    // 2. Create real filter from it's holder.
                    var filter = (IContinuousQueryFilter)DelegateTypeDescriptor.GetContinuousQueryFilterCtor(
                        filterHolder.Filter.GetType())(filterHolder.Filter, filterHolder.KeepBinary);

                    // 3. Inject grid.
                    filter.Inject(_ignite);

                    // 4. Allocate GC handle.
                    return filter.Allocate();
                }
            });
        }

        private int ContinuousQueryFilterApply(long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var holder = _handleRegistry.Get<IContinuousQueryFilter>(stream.ReadLong());

                    return holder.Evaluate(stream) ? 1 : 0;
                }
            });
        }

        private void ContinuousQueryFilterRelease(long filterPtr)
        {
            SafeCall(() =>
            {
                var holder = _handleRegistry.Get<IContinuousQueryFilter>(filterPtr);

                holder.Release();
            });
        }

        #endregion

        #region IMPLEMENTATION: DATA STREAMER

        private void DataStreamerTopologyUpdate(long ldrPtr, long topVer, int topSize)
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
        private void DataStreamerStreamReceiverInvoke(void* cache, long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var rcvPtr = stream.ReadLong();

                    var keepBinary = stream.ReadBool();

                    var reader = _ignite.Marshaller.StartUnmarshal(stream, BinaryMode.ForceBinary);

                    var binaryReceiver = reader.ReadObject<BinaryObject>();

                    var receiver = _handleRegistry.Get<StreamReceiverHolder>(rcvPtr) ??
                                   binaryReceiver.Deserialize<StreamReceiverHolder>();

                    if (receiver != null)
                        receiver.Receive(_ignite, new UnmanagedNonReleaseableTarget(_ctx, cache), stream, keepBinary);
                }
            });
        }

        #endregion

        #region IMPLEMENTATION: FUTURES

        private void FutureByteResult(long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<byte>(futPtr, fut => { fut.OnResult((byte)res); });
            });
        }

        private void FutureBoolResult(long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<bool>(futPtr, fut => { fut.OnResult(res == 1); });
            });
        }

        private void FutureShortResult(long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<short>(futPtr, fut => { fut.OnResult((short)res); });
            });
        }

        private void FutureCharResult(long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<char>(futPtr, fut => { fut.OnResult((char)res); });
            });
        }

        private void FutureIntResult(long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<int>(futPtr, fut => { fut.OnResult((int) res); });
            });
        }

        private void FutureFloatResult(long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<float>(futPtr, fut => { fut.OnResult(BinaryUtils.IntToFloatBits((int) res)); });
            });
        }

        private void FutureLongResult(long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<long>(futPtr, fut => { fut.OnResult(res); });
            });
        }

        private void FutureDoubleResult(long futPtr, long res)
        {
            SafeCall(() =>
            {
                ProcessFuture<double>(futPtr, fut => { fut.OnResult(BinaryUtils.LongToDoubleBits(res)); });
            });
        }

        private void FutureObjectResult(long futPtr, long memPtr)
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

        private void FutureNullResult(long futPtr)
        {
            SafeCall(() =>
            {
                ProcessFuture(futPtr, fut => { fut.OnNullResult(); });
            });
        }

        private void FutureError(long futPtr, long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    string errCls = reader.ReadString();
                    string errMsg = reader.ReadString();
                    string stackTrace = reader.ReadString();
                    Exception inner = reader.ReadBoolean() ? reader.ReadObject<Exception>() : null;

                    Exception err = ExceptionUtils.GetException(_ignite, errCls, errMsg, stackTrace, reader, inner);

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

        private void LifecycleOnEvent(long ptr, long evt)
        {
            SafeCall(() =>
            {
                var bean = _handleRegistry.Get<LifecycleBeanHolder>(ptr);

                bean.OnLifecycleEvent((LifecycleEventType)evt);
            }, true);
        }

        #endregion

        #region IMPLEMENTATION: MESSAGING

        private long MessagingFilterCreate(long memPtr)
        {
            return SafeCall(() =>
            {
                MessageListenerHolder holder = MessageListenerHolder.CreateRemote(_ignite, memPtr);

                return _ignite.HandleRegistry.AllocateSafe(holder);
            });
        }

        private int MessagingFilterApply(long ptr, long memPtr)
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

        private void MessagingFilterDestroy(long ptr)
        {
            SafeCall(() =>
            {
                _ignite.HandleRegistry.Release(ptr);
            });
        }

        #endregion

        #region IMPLEMENTATION: EXTENSIONS

        private long ExtensionCallbackInLongOutLong(int op)
        {
            throw new InvalidOperationException("Unsupported operation type: " + op);
        }

        private long ExtensionCallbackInLongLongOutLong(int op, long arg1, long arg2)
        {
            return SafeCall(() =>
            {
                switch (op)
                {
                    case OpPrepareDotNet:
                        using (var inStream = IgniteManager.Memory.Get(arg1).GetStream())
                        using (var outStream = IgniteManager.Memory.Get(arg2).GetStream())
                        {
                            Ignition.OnPrepare(inStream, outStream, _handleRegistry, _log);

                            return 0;
                        }

                    default:
                        throw new InvalidOperationException("Unsupported operation type: " + op);
                }
            }, op == OpPrepareDotNet);
        }

        #endregion

        #region IMPLEMENTATION: EVENTS

        private long EventFilterCreate(long memPtr)
        {
            return SafeCall(() => _handleRegistry.Allocate(RemoteListenEventFilter.CreateInstance(memPtr, _ignite)));
        }

        private int EventFilterApply(long ptr, long memPtr)
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

        private void EventFilterDestroy(long ptr)
        {
            SafeCall(() =>
            {
                _ignite.HandleRegistry.Release(ptr);
            });
        }

        #endregion

        #region IMPLEMENTATION: SERVICES

        private long ServiceInit(long memPtr)
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

        private void ServiceExecute(long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var svc = _handleRegistry.Get<IService>(stream.ReadLong());

                    // Ignite does not guarantee that Cancel is called after Execute exits
                    // So missing handle is a valid situation
                    if (svc == null)
                        return;

                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    bool srvKeepBinary = reader.ReadBoolean();

                    svc.Execute(new ServiceContext(_ignite.Marshaller.StartUnmarshal(stream, srvKeepBinary)));
                }
            });
        }

        private void ServiceCancel(long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    long svcPtr = stream.ReadLong();

                    try
                    {
                        var svc = _handleRegistry.Get<IService>(svcPtr, true);

                        var reader = _ignite.Marshaller.StartUnmarshal(stream);

                        bool srvKeepBinary = reader.ReadBoolean();

                        svc.Cancel(new ServiceContext(_ignite.Marshaller.StartUnmarshal(stream, srvKeepBinary)));
                    }
                    finally
                    {
                        _ignite.HandleRegistry.Release(svcPtr);
                    }
                }
            });
        }

        private void ServiceInvokeMethod(long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var svc = _handleRegistry.Get<IService>(stream.ReadLong(), true);

                    string mthdName;
                    object[] mthdArgs;

                    ServiceProxySerializer.ReadProxyMethod(stream, _ignite.Marshaller, out mthdName, out mthdArgs);

                    var result = ServiceProxyInvoker.InvokeServiceMethod(svc, mthdName, mthdArgs);

                    stream.Reset();

                    ServiceProxySerializer.WriteInvocationResult(stream, _ignite.Marshaller, result.Key, result.Value);

                    stream.SynchronizeOutput();
                }
            });
        }

        private int ClusterNodeFilterApply(long memPtr)
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

        private void NodeInfo(long memPtr)
        {
            SafeCall(() => _ignite.UpdateNodeInfo(memPtr));
        }

        private void MemoryReallocate(long memPtr, int cap)
        {
            SafeCall(() =>
            {
                IgniteManager.Memory.Get(memPtr).Reallocate(cap);
            }, true);
        }

        private void OnStart(void* proc, long memPtr)
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

        private void OnStop()
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
            int errMsgCharsLen, sbyte* stackTraceChars, int stackTraceCharsLen, void* errData, int errDataLen)
        {
            // errData mechanism is only needed for CachePartialUpdateException and is no longer used,
            // since CacheImpl handles all errors itself.
            Debug.Assert(errDataLen == 0);
            Debug.Assert(errData == null);

            string errCls = IgniteUtils.Utf8UnmanagedToString(errClsChars, errClsCharsLen);
            string errMsg = IgniteUtils.Utf8UnmanagedToString(errMsgChars, errMsgCharsLen);
            string stackTrace = IgniteUtils.Utf8UnmanagedToString(stackTraceChars, stackTraceCharsLen);

            switch (errType)
            {
                case ErrGeneric:
                    throw ExceptionUtils.GetException(_ignite, errCls, errMsg, stackTrace);

                case ErrJvmInit:
                    throw ExceptionUtils.GetJvmInitializeException(errCls, errMsg, stackTrace);

                case ErrJvmAttach:
                    throw new IgniteException("Failed to attach to JVM.");

                default:
                    throw new IgniteException("Unknown exception [cls=" + errCls + ", msg=" + errMsg + ']');
            }
        }

        private void OnClientDisconnected()
        {
            SafeCall(() =>
            {
                _ignite.OnClientDisconnected();
            });
        }

        private void OnClientReconnected(bool clusterRestarted)
        {
            SafeCall(() =>
            {
                _ignite.OnClientReconnected(clusterRestarted);
            });
        }

        private void LoggerLog(void* target, int level, sbyte* messageChars, int messageCharsLen, sbyte* categoryChars,
            int categoryCharsLen, sbyte* errorInfoChars, int errorInfoCharsLen, long memPtr)
        {
            // When custom logger in .NET is not defined, Java should not call us.
            Debug.Assert(!(_log is JavaLogger));

            SafeCall(() =>
            {
                var message = IgniteUtils.Utf8UnmanagedToString(messageChars, messageCharsLen);
                var category = IgniteUtils.Utf8UnmanagedToString(categoryChars, categoryCharsLen);
                var nativeError = IgniteUtils.Utf8UnmanagedToString(errorInfoChars, errorInfoCharsLen);

                Exception ex = null;

                if (memPtr != 0 && _ignite != null)
                {
                    using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                    {
                        ex = _ignite.Marshaller.Unmarshal<Exception>(stream);
                    }
                }

                _log.Log((LogLevel) level, message, null, CultureInfo.InvariantCulture, category, nativeError, ex);
            }, true);
        }

        private bool LoggerIsLevelEnabled(void* target, int level)
        {
            // When custom logger in .NET is not defined, Java should not call us.
            Debug.Assert(!(_log is JavaLogger));

            return SafeCall(() => _log.IsEnabled((LogLevel) level), true);
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static void ConsoleWrite(sbyte* chars, int charsLen, bool isErr)
        {
            try
            {
                var str = IgniteUtils.Utf8UnmanagedToString(chars, charsLen);

                var target = isErr ? Console.Error : Console.Out;

                target.Write(str);

            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("ConsoleWrite unmanaged callback failed: " + ex);
            }
        }

        #endregion

        #region AffinityFunction

        private long AffinityFunctionInit(long memPtr, void* baseFunc)
        {
            return SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    var func = reader.ReadObjectEx<IAffinityFunction>();

                    ResourceProcessor.Inject(func, _ignite);

                    var affBase = func as AffinityFunctionBase;

                    if (affBase != null)
                        affBase.SetBaseFunction(new PlatformAffinityFunction(
                            _ignite.InteropProcessor.ChangeTarget(baseFunc), _ignite.Marshaller));

                    return _handleRegistry.Allocate(func);
                }
            });
        }

        private int AffinityFunctionPartition(long memPtr)
        {
            return SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var ptr = stream.ReadLong();

                    var key = _ignite.Marshaller.Unmarshal<object>(stream);

                    return _handleRegistry.Get<IAffinityFunction>(ptr, true).GetPartition(key);
                }
            });
        }

        private void AffinityFunctionAssignPartitions(long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var ptr = stream.ReadLong();
                    var ctx = new AffinityFunctionContext(_ignite.Marshaller.StartUnmarshal(stream));
                    var func = _handleRegistry.Get<IAffinityFunction>(ptr, true);
                    var parts = func.AssignPartitions(ctx);

                    if (parts == null)
                        throw new IgniteException(func.GetType() + ".AssignPartitions() returned invalid result: null");

                    stream.Reset();

                    AffinityFunctionSerializer.WritePartitions(parts, stream, _ignite.Marshaller);
                }
            });
        }

        private void AffinityFunctionRemoveNode(long memPtr)
        {
            SafeCall(() =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    var ptr = stream.ReadLong();
                    var nodeId = _ignite.Marshaller.Unmarshal<Guid>(stream);

                    _handleRegistry.Get<IAffinityFunction>(ptr, true).RemoveNode(nodeId);
                }
            });
        }

        private void AffinityFunctionDestroy(long ptr)
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
                _log.Error(e, "Failure in Java callback");

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
                _log.Error(e, "Failure in Java callback");

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
        /// Gets the log.
        /// </summary>
        public ILogger Log
        {
            get { return _log; }
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

            ResourceProcessor.Inject(_log, grid);
        }

        /// <summary>
        /// Cleanups this instance.
        /// </summary>
        public void Cleanup()
        {
            _ignite = null;

            _handleRegistry.Close();
        }

        /// <summary>
        /// Gets the console write handler.
        /// </summary>
        public static void* ConsoleWriteHandler
        {
            get { return ConsoleWritePtr; }
        }
    }
}