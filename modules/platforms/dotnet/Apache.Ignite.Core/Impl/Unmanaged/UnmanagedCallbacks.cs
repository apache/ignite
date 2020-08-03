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
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
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
    using Apache.Ignite.Core.Impl.Unmanaged.Jni;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Services;
    using UU = UnmanagedUtils;

    /// <summary>
    /// Unmanaged callbacks.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable",
        Justification = "This class instance usually lives as long as the app runs.")]
    [SuppressMessage("Microsoft.Design", "CA1049:TypesThatOwnNativeResourcesShouldBeDisposable",
        Justification = "This class instance usually lives as long as the app runs.")]
    internal unsafe class UnmanagedCallbacks : MarshalByRefObject
    {
        /** */
        private long _igniteId;

        /** Handle registry. */
        private readonly HandleRegistry _handleRegistry = new HandleRegistry();

        /** JVM. */
        private readonly Jvm _jvm;

        /** Grid. */
        private volatile Ignite _ignite;

        /** Log. */
        private volatile ILogger _log;

        /** Max op code. */
        private static readonly int MaxOpCode = Enum.GetValues(typeof(UnmanagedCallbackOp)).Cast<int>().Max();

        /** Handlers array. */
        private readonly InLongOutLongHandler[] _inLongOutLongHandlers = new InLongOutLongHandler[MaxOpCode + 1];

        /** Handlers array. */
        private readonly InLongLongLongObjectOutLongHandler[] _inLongLongLongObjectOutLongHandlers
            = new InLongLongLongObjectOutLongHandler[MaxOpCode + 1];

        /** Initialized flag. */
        private readonly ManualResetEventSlim _initEvent = new ManualResetEventSlim(false);

        /** Actions to be called upon Ignite initialization. */
        private readonly List<Action<Ignite>> _initActions = new List<Action<Ignite>>();

        /** Operation: prepare .Net. */
        private const int OpPrepareDotNet = 1;

        private delegate long InLongOutLongFunc(long val);
        private delegate long InLongLongLongObjectOutLongFunc(long val1, long val2, long val3, void* arg);

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="log">Logger.</param>
        /// <param name="jvm"></param>
        public UnmanagedCallbacks(ILogger log, Jvm jvm)
        {
            Debug.Assert(log != null);
            Debug.Assert(jvm != null);

            _log = log;
            _jvm = jvm;

            InitHandlers();
        }

        /** <inheritdoc /> */
        public override object InitializeLifetimeService()
        {
            // Ensure that cross-AppDomain reference lives forever.
            return null;
        }

        /// <summary>
        /// Gets the handle registry.
        /// </summary>
        public HandleRegistry HandleRegistry
        {
            get { return _handleRegistry; }
        }

        /// <summary>
        /// Gets the ignite identifier.
        /// </summary>
        public long IgniteId
        {
            get { return _igniteId; }
        }

        /// <summary>
        /// Gets the JVM.
        /// </summary>
        public Jvm Jvm
        {
            get { return _jvm; }
        }

        #region HANDLERS

        /// <summary>
        /// Initializes the handlers.
        /// </summary>
        private void InitHandlers()
        {
            AddHandler(UnmanagedCallbackOp.CacheStoreCreate, CacheStoreCreate, true);
            AddHandler(UnmanagedCallbackOp.CacheStoreInvoke, CacheStoreInvoke);
            AddHandler(UnmanagedCallbackOp.CacheStoreDestroy, CacheStoreDestroy);
            AddHandler(UnmanagedCallbackOp.CacheStoreSessionCreate, CacheStoreSessionCreate);
            AddHandler(UnmanagedCallbackOp.CacheEntryFilterCreate, CacheEntryFilterCreate);
            AddHandler(UnmanagedCallbackOp.CacheEntryFilterApply, CacheEntryFilterApply);
            AddHandler(UnmanagedCallbackOp.CacheEntryFilterDestroy, CacheEntryFilterDestroy);
            AddHandler(UnmanagedCallbackOp.CacheInvoke, CacheInvoke);
            AddHandler(UnmanagedCallbackOp.ComputeTaskMap, ComputeTaskMap);
            AddHandler(UnmanagedCallbackOp.ComputeTaskJobResult, ComputeTaskJobResult);
            AddHandler(UnmanagedCallbackOp.ComputeTaskReduce, ComputeTaskReduce);
            AddHandler(UnmanagedCallbackOp.ComputeTaskComplete, ComputeTaskComplete);
            AddHandler(UnmanagedCallbackOp.ComputeJobSerialize, ComputeJobSerialize);
            AddHandler(UnmanagedCallbackOp.ComputeJobCreate, ComputeJobCreate);
            AddHandler(UnmanagedCallbackOp.ComputeJobExecute, ComputeJobExecute);
            AddHandler(UnmanagedCallbackOp.ComputeJobCancel, ComputeJobCancel);
            AddHandler(UnmanagedCallbackOp.ComputeJobDestroy, ComputeJobDestroy);
            AddHandler(UnmanagedCallbackOp.ContinuousQueryListenerApply, ContinuousQueryListenerApply);
            AddHandler(UnmanagedCallbackOp.ContinuousQueryFilterCreate, ContinuousQueryFilterCreate);
            AddHandler(UnmanagedCallbackOp.ContinuousQueryFilterApply, ContinuousQueryFilterApply);
            AddHandler(UnmanagedCallbackOp.ContinuousQueryFilterRelease, ContinuousQueryFilterRelease);
            AddHandler(UnmanagedCallbackOp.DataStreamerTopologyUpdate, DataStreamerTopologyUpdate);
            AddHandler(UnmanagedCallbackOp.DataStreamerStreamReceiverInvoke, DataStreamerStreamReceiverInvoke);
            AddHandler(UnmanagedCallbackOp.FutureByteResult, FutureByteResult);
            AddHandler(UnmanagedCallbackOp.FutureBoolResult, FutureBoolResult);
            AddHandler(UnmanagedCallbackOp.FutureShortResult, FutureShortResult);
            AddHandler(UnmanagedCallbackOp.FutureCharResult, FutureCharResult);
            AddHandler(UnmanagedCallbackOp.FutureIntResult, FutureIntResult);
            AddHandler(UnmanagedCallbackOp.FutureFloatResult, FutureFloatResult);
            AddHandler(UnmanagedCallbackOp.FutureLongResult, FutureLongResult);
            AddHandler(UnmanagedCallbackOp.FutureDoubleResult, FutureDoubleResult);
            AddHandler(UnmanagedCallbackOp.FutureObjectResult, FutureObjectResult);
            AddHandler(UnmanagedCallbackOp.FutureNullResult, FutureNullResult);
            AddHandler(UnmanagedCallbackOp.FutureError, FutureError);
            AddHandler(UnmanagedCallbackOp.LifecycleOnEvent, LifecycleOnEvent, true);
            AddHandler(UnmanagedCallbackOp.MemoryReallocate, MemoryReallocate, true);
            AddHandler(UnmanagedCallbackOp.MessagingFilterCreate, MessagingFilterCreate);
            AddHandler(UnmanagedCallbackOp.MessagingFilterApply, MessagingFilterApply);
            AddHandler(UnmanagedCallbackOp.MessagingFilterDestroy, MessagingFilterDestroy);
            AddHandler(UnmanagedCallbackOp.EventFilterCreate, EventFilterCreate);
            AddHandler(UnmanagedCallbackOp.EventFilterApply, EventFilterApply);
            AddHandler(UnmanagedCallbackOp.EventFilterDestroy, EventFilterDestroy);
            AddHandler(UnmanagedCallbackOp.EventLocalListenerApply, EventLocalListenerApply);
            AddHandler(UnmanagedCallbackOp.ServiceInit, ServiceInit);
            AddHandler(UnmanagedCallbackOp.ServiceExecute, ServiceExecute);
            AddHandler(UnmanagedCallbackOp.ServiceCancel, ServiceCancel);
            AddHandler(UnmanagedCallbackOp.ServiceInvokeMethod, ServiceInvokeMethod);
            AddHandler(UnmanagedCallbackOp.ClusterNodeFilterApply, ClusterNodeFilterApply);
            AddHandler(UnmanagedCallbackOp.NodeInfo, NodeInfo);
            AddHandler(UnmanagedCallbackOp.OnStart, OnStart, true);
            AddHandler(UnmanagedCallbackOp.OnStop, OnStop, true);
            AddHandler(UnmanagedCallbackOp.ExtensionInLongLongOutLong, ExtensionCallbackInLongLongOutLong, true);
            AddHandler(UnmanagedCallbackOp.OnClientDisconnected, OnClientDisconnected);
            AddHandler(UnmanagedCallbackOp.OnClientReconnected, OnClientReconnected);
            AddHandler(UnmanagedCallbackOp.AffinityFunctionInit, AffinityFunctionInit);
            AddHandler(UnmanagedCallbackOp.AffinityFunctionPartition, AffinityFunctionPartition);
            AddHandler(UnmanagedCallbackOp.AffinityFunctionAssignPartitions, AffinityFunctionAssignPartitions);
            AddHandler(UnmanagedCallbackOp.AffinityFunctionRemoveNode, AffinityFunctionRemoveNode);
            AddHandler(UnmanagedCallbackOp.AffinityFunctionDestroy, AffinityFunctionDestroy);
            AddHandler(UnmanagedCallbackOp.ComputeTaskLocalJobResult, ComputeTaskLocalJobResult);
            AddHandler(UnmanagedCallbackOp.ComputeJobExecuteLocal, ComputeJobExecuteLocal);
            AddHandler(UnmanagedCallbackOp.PluginProcessorStop, PluginProcessorStop);
            AddHandler(UnmanagedCallbackOp.PluginProcessorIgniteStop, PluginProcessorIgniteStop);
            AddHandler(UnmanagedCallbackOp.PluginCallbackInLongLongOutLong, PluginCallbackInLongLongOutLong);
            AddHandler(UnmanagedCallbackOp.PlatformCacheUpdate, PlatformCacheUpdate);
            AddHandler(UnmanagedCallbackOp.PlatformCacheUpdateFromThreadLocal, PlatformCacheUpdateFromThreadLocal);
            AddHandler(UnmanagedCallbackOp.OnCacheStopped, OnCacheStopped);
            AddHandler(UnmanagedCallbackOp.OnAffinityTopologyVersionChanged, OnAffinityTopologyVersionChanged);
            AddHandler(UnmanagedCallbackOp.ComputeOutFuncExecute, ComputeOutFuncExecute);
            AddHandler(UnmanagedCallbackOp.ComputeActionExecute, ComputeActionExecute);
        }

        /// <summary>
        /// Adds the handler.
        /// </summary>
        private void AddHandler(UnmanagedCallbackOp op, InLongOutLongFunc func, bool allowUninitialized = false)
        {
            _inLongOutLongHandlers[(int)op] = new InLongOutLongHandler(func, allowUninitialized);
        }

        /// <summary>
        /// Adds the handler.
        /// </summary>
        private void AddHandler(UnmanagedCallbackOp op, InLongLongLongObjectOutLongFunc func, 
            bool allowUninitialized = false)
        {
            _inLongLongLongObjectOutLongHandlers[(int)op] 
                = new InLongLongLongObjectOutLongHandler(func, allowUninitialized);
        }

        #endregion

        #region IMPLEMENTATION: GENERAL PURPOSE

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal long InLongOutLong(int type, long val)
        {
            try
            {
                if (type < 0 || type > _inLongOutLongHandlers.Length)
                    throw GetInvalidOpError("InLongOutLong", type);

                var hnd = _inLongOutLongHandlers[type];

                if (hnd.Handler == null)
                    throw GetInvalidOpError("InLongOutLong", type);

                if (!hnd.AllowUninitialized)
                    _initEvent.Wait();

                return hnd.Handler(val);
            }
            catch (Exception e)
            {
                _log.Error(e, "Failure in Java callback");
                throw;
            }
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal long InLongLongLongObjectOutLong(int type, long val1, long val2, long val3, IntPtr arg)
        {
            try
            {
                if (type < 0 || type > _inLongLongLongObjectOutLongHandlers.Length)
                    throw GetInvalidOpError("InLongLongLongObjectOutLong", type);

                var hnd = _inLongLongLongObjectOutLongHandlers[type];

                if (hnd.Handler == null)
                    throw GetInvalidOpError("InLongLongLongObjectOutLong", type);

                if (!hnd.AllowUninitialized)
                    _initEvent.Wait();

                return hnd.Handler(val1, val2, val3, arg.ToPointer());
            }
            catch (Exception e)
            {
                _log.Error(e, "Failure in Java callback");
                throw;
            }
        }

        /// <summary>
        /// Throws the invalid op error.
        /// </summary>
        private static Exception GetInvalidOpError(string method, int type)
        {
            return new InvalidOperationException(
                string.Format("Invalid {0} callback code: {1}", method, (UnmanagedCallbackOp) type));
        }

        #endregion

        #region IMPLEMENTATION: CACHE

        private long CacheStoreCreate(long memPtr)
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
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private long CacheStoreInvoke(long memPtr)
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

                    var writer = _ignite.Marshaller.StartMarshal(stream);

                    writer.WriteObject(e);

                    _ignite.Marshaller.FinishMarshal(writer);

                    return -1;
                }
            }
        }

        private long CacheStoreDestroy(long objPtr)
        {
            _ignite.HandleRegistry.Release(objPtr);

            return 0;
        }

        private long CacheStoreSessionCreate(long val)
        {
            return _ignite.HandleRegistry.Allocate(new CacheStoreSession());
        }

        private long CacheEntryFilterCreate(long memPtr)
        {
            return _handleRegistry.Allocate(CacheEntryFilterHolder.CreateInstance(memPtr, _ignite));
        }

        private long CacheEntryFilterApply(long memPtr)
        {
            using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var t = _ignite.HandleRegistry.Get<CacheEntryFilterHolder>(stream.ReadLong(), true);

                Debug.Assert(t != null);

                return t.Invoke(stream);
            }
        }

        private long CacheEntryFilterDestroy(long objPtr)
        {
            _ignite.HandleRegistry.Release(objPtr);

            return 0;
        }

        private long CacheInvoke(long memPtr)
        {
            using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var result = ReadAndRunCacheEntryProcessor(stream, _ignite);

                stream.Reset();

                result.Write(stream, _ignite.Marshaller);

                stream.SynchronizeOutput();
            }

            return 0;
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

            var isLocal = inOutStream.ReadBool();

            var holder = isLocal
                ? _handleRegistry.Get<CacheEntryProcessorHolder>(inOutStream.ReadLong(), true)
                : marsh.Unmarshal<CacheEntryProcessorHolder>(inOutStream);

            var key = marsh.Unmarshal<object>(inOutStream);
            var val = marsh.Unmarshal<object>(inOutStream);

            return holder.Process(key, val, val != null, grid);
        }
        
        /// <summary>
        /// Updates platform cache entry.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <returns>Unused.</returns>
        private long PlatformCacheUpdate(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var cacheId = stream.ReadInt();

                _ignite.PlatformCacheManager.Update(cacheId, stream, _ignite.Marshaller);
            }

            return 0;
        }
        
        /// <summary>
        /// Updates platform cache entry.
        /// </summary>
        private long PlatformCacheUpdateFromThreadLocal(long cacheIdAndPartition, long verMajor, long verMinor, void* arg)
        {
            int cacheId = (int)(cacheIdAndPartition & 0xFFFFFFFF);
            int partition = (int) (cacheIdAndPartition >> 32);

            _ignite.PlatformCacheManager.UpdateFromThreadLocal(
                cacheId, partition, new AffinityTopologyVersion(verMajor, (int) verMinor));
                
            return 0;
        }
        
        /// <summary>
        /// Called on cache stop.
        /// </summary>
        /// <param name="cacheId">Cache id.</param>
        private long OnCacheStopped(long cacheId)
        {
            _ignite.PlatformCacheManager.Stop((int) cacheId);
            
            return 0;
        }
        
        /// <summary>
        /// Called on affinity topology version change.
        /// </summary>
        private long OnAffinityTopologyVersionChanged(
            long topologyVersion, long minorTopologyVersion, long unused, void* arg)
        {
            var affinityTopologyVersion = new AffinityTopologyVersion(topologyVersion, (int) minorTopologyVersion);
            
            _ignite.PlatformCacheManager.OnAffinityTopologyVersionChanged(affinityTopologyVersion);
            
            return 0;
        }

        #endregion

        #region IMPLEMENTATION: COMPUTE

        private long ComputeTaskMap(long memPtr)
        {
            using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                Task(stream.ReadLong()).Map(stream);

                return 0;
            }
        }

        private long ComputeTaskLocalJobResult(long taskPtr, long jobPtr, long unused, void* arg)
        {
            return Task(taskPtr).JobResultLocal(Job(jobPtr));
        }

        private long ComputeTaskJobResult(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var task = Task(stream.ReadLong());

                var job = Job(stream.ReadLong());

                return task.JobResultRemote(job, stream);
            }
        }

        private long ComputeTaskReduce(long taskPtr)
        {
            _handleRegistry.Get<IComputeTaskHolder>(taskPtr, true).Reduce();

            return 0;
        }

        private long ComputeTaskComplete(long taskPtr, long memPtr, long unused, void* arg)
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

            return 0;
        }

        private long ComputeJobSerialize(long jobPtr, long memPtr, long unused, void* arg)
        {
            using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                return Job(jobPtr).Serialize(stream) ? 1 : 0;
            }
        }

        private long ComputeJobCreate(long memPtr)
        {
            using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                ComputeJobHolder job = ComputeJobHolder.CreateJob(_ignite, stream);

                return _handleRegistry.Allocate(job);
            }
        }

        private long ComputeJobExecuteLocal(long jobPtr, long cancel, long unused, void* arg)
        {
            Job(jobPtr).ExecuteLocal(cancel == 1);

            return 0;
        }

        private long ComputeJobExecute(long memPtr)
        {
            using (PlatformMemoryStream stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var job = Job(stream.ReadLong());

                var cancel = stream.ReadBool();

                stream.Reset();

                job.ExecuteRemote(stream, cancel);
            }

            return 0;
        }

        private long ComputeJobCancel(long jobPtr)
        {
            var job = Job(jobPtr);

            // Job can be null: some jobs are destroyed in ComputeTaskComplete
            if (job != null)
            {
                job.Cancel();
            }

            return 0;
        }

        private long ComputeJobDestroy(long jobPtr)
        {
            _handleRegistry.Release(jobPtr);

            return 0;
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
        
        /// <summary>
        /// Executes <see cref="IComputeOutFunc"/>.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        private long ComputeOutFuncExecute(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var func = stream.ReadBool()
                    ? _handleRegistry.Get<object>(stream.ReadLong(), true)
                    : _ignite.Marshaller.Unmarshal<object>(stream);
                
                stream.Reset();
                
                var invoker = DelegateTypeDescriptor.GetComputeOutFunc(func.GetType());
                ComputeRunner.ExecuteJobAndWriteResults(_ignite, stream, func, invoker);
            }

            return 0;
        }

        /// <summary>
        /// Executes <see cref="IComputeAction"/>.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        private long ComputeActionExecute(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var action = stream.ReadBool()
                    ? _handleRegistry.Get<IComputeAction>(stream.ReadLong(), true)
                    : _ignite.Marshaller.Unmarshal<IComputeAction>(stream);
                
                stream.Reset();
                
                ComputeRunner.ExecuteJobAndWriteResults(_ignite, stream, action, act =>
                {
                    act.Invoke();
                    return null;
                });
            }

            return 0;
        }

        #endregion

        #region  IMPLEMENTATION: CONTINUOUS QUERY

        private long ContinuousQueryListenerApply(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var hnd = _handleRegistry.Get<IContinuousQueryHandleImpl>(stream.ReadLong());

                hnd.Apply(stream);

                return 0;
            }
        }

        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        private long ContinuousQueryFilterCreate(long memPtr)
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
        }

        private long ContinuousQueryFilterApply(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var holder = _handleRegistry.Get<IContinuousQueryFilter>(stream.ReadLong());

                return holder.Evaluate(stream) ? 1 : 0;
            }
        }

        private long ContinuousQueryFilterRelease(long filterPtr)
        {
            var holder = _handleRegistry.Get<IContinuousQueryFilter>(filterPtr);

            holder.Release();

            return 0;
        }

        #endregion

        #region IMPLEMENTATION: DATA STREAMER

        private long DataStreamerTopologyUpdate(long ldrPtr, long topVer, long topSize, void* unused)
        {
            var ldrRef = _handleRegistry.Get<WeakReference>(ldrPtr);

            if (ldrRef == null)
                return 0;

            var ldr = ldrRef.Target as IDataStreamer;

            if (ldr != null)
                ldr.TopologyChange(topVer, (int) topSize);
            else
                _handleRegistry.Release(ldrPtr, true);

            return 0;
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private long DataStreamerStreamReceiverInvoke(long memPtr, long unused, long unused1, void* cache)
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
                {
                    var cacheRef = _jvm.AttachCurrentThread().NewGlobalRef((IntPtr) cache);
                    var target = new PlatformJniTarget(cacheRef, _ignite.Marshaller);
                    receiver.Receive(_ignite, target, stream, keepBinary);
                }

                return 0;
            }
        }

        #endregion

        #region IMPLEMENTATION: FUTURES

        private long FutureByteResult(long futPtr, long res, long unused, void* arg)
        {
            return ProcessFuture<byte>(futPtr, fut => { fut.OnResult((byte) res); });
        }

        private long FutureBoolResult(long futPtr, long res, long unused, void* arg)
        {
            return ProcessFuture<bool>(futPtr, fut => { fut.OnResult(res == 1); });
        }

        private long FutureShortResult(long futPtr, long res, long unused, void* arg)
        {
            return ProcessFuture<short>(futPtr, fut => { fut.OnResult((short)res); });
        }

        private long FutureCharResult(long futPtr, long res, long unused, void* arg)
        {
            return ProcessFuture<char>(futPtr, fut => { fut.OnResult((char)res); });
        }

        private long FutureIntResult(long futPtr, long res, long unused, void* arg)
        {
            return ProcessFuture<int>(futPtr, fut => { fut.OnResult((int) res); });
        }

        private long FutureFloatResult(long futPtr, long res, long unused, void* arg)
        {
            return ProcessFuture<float>(futPtr, fut => { fut.OnResult(BinaryUtils.IntToFloatBits((int) res)); });
        }

        private long FutureLongResult(long futPtr, long res, long unused, void* arg)
        {
            return ProcessFuture<long>(futPtr, fut => { fut.OnResult(res); });
        }

        private long FutureDoubleResult(long futPtr, long res, long unused, void* arg)
        {
            return ProcessFuture<double>(futPtr, fut => { fut.OnResult(BinaryUtils.LongToDoubleBits(res)); });
        }

        private long FutureObjectResult(long futPtr, long memPtr, long unused, void* arg)
        {
            return ProcessFuture(futPtr, fut =>
            {
                using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
                {
                    fut.OnResult(stream);
                }
            });
        }

        private long FutureNullResult(long futPtr)
        {
            return ProcessFuture(futPtr, fut => { fut.OnNullResult(); });
        }

        private long FutureError(long futPtr, long memPtr, long unused, void* arg)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var reader = _ignite.Marshaller.StartUnmarshal(stream);

                string errCls = reader.ReadString();
                string errMsg = reader.ReadString();
                string stackTrace = reader.ReadString();
                Exception inner = reader.ReadBoolean() ? reader.ReadObject<Exception>() : null;

                Exception err = ExceptionUtils.GetException(_ignite, errCls, errMsg, stackTrace, reader, inner);

                return ProcessFuture(futPtr, fut => { fut.OnError(err); });
            }
        }

        /// <summary>
        /// Process future.
        /// </summary>
        /// <param name="futPtr">Future pointer.</param>
        /// <param name="action">Action.</param>
        private long ProcessFuture(long futPtr, Action<IFutureInternal> action)
        {
            try
            {
                action(_handleRegistry.Get<IFutureInternal>(futPtr, true));

                return 0;
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
        private long ProcessFuture<T>(long futPtr, Action<Future<T>> action)
        {
            try
            {
                action(_handleRegistry.Get<Future<T>>(futPtr, true));

                return 0;
            }
            finally
            {
                _handleRegistry.Release(futPtr);
            }
        }

        #endregion

        #region IMPLEMENTATION: LIFECYCLE

        private long LifecycleOnEvent(long ptr, long evt, long unused, void* arg)
        {
            var bean = _handleRegistry.Get<LifecycleHandlerHolder>(ptr);

            bean.OnLifecycleEvent((LifecycleEventType) evt);

            return 0;
        }

        #endregion

        #region IMPLEMENTATION: MESSAGING

        private long MessagingFilterCreate(long memPtr)
        {
            MessageListenerHolder holder = MessageListenerHolder.CreateRemote(_ignite, memPtr);

            return _ignite.HandleRegistry.AllocateSafe(holder);
        }

        private long MessagingFilterApply(long ptr, long memPtr, long unused, void* arg)
        {
            var holder = _ignite.HandleRegistry.Get<MessageListenerHolder>(ptr, false);

            if (holder == null)
                return 0;

            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                return holder.Invoke(stream);
            }
        }

        private long MessagingFilterDestroy(long ptr)
        {
            _ignite.HandleRegistry.Release(ptr);

            return 0;
        }

        #endregion

        #region IMPLEMENTATION: EXTENSIONS

        private long ExtensionCallbackInLongLongOutLong(long op, long arg1, long arg2, void* arg)
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
        }

        #endregion

        #region IMPLEMENTATION: EVENTS

        private long EventFilterCreate(long memPtr)
        {
            return _handleRegistry.Allocate(RemoteListenEventFilter.CreateInstance(memPtr, _ignite));
        }

        private long EventFilterApply(long ptr, long memPtr, long unused, void* arg)
        {
            var holder = _ignite.HandleRegistry.Get<IInteropCallback>(ptr, false);

            if (holder == null)
                return 0;

            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                return holder.Invoke(stream);
            }
        }

        private long EventFilterDestroy(long ptr)
        {
            _ignite.HandleRegistry.Release(ptr);

            return 0;
        }

        private long EventLocalListenerApply(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var id = stream.ReadInt();

                var listeners = _ignite.Configuration.LocalEventListenersInternal;

                if (listeners == null || id >= listeners.Length)
                {
                    return 0;
                }

                var listener = listeners[id];

                var reader = _ignite.Marshaller.StartUnmarshal(stream);

                var res = listener.Invoke(reader);

                return res ? 1 : 0;
            }
        }

        #endregion

        #region IMPLEMENTATION: SERVICES

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "User processor can throw any exception")]
        private long ServiceInit(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                try
                {
                    var reader = _ignite.Marshaller.StartUnmarshal(stream);

                    var srvKeepBinary = reader.ReadBoolean();
                    var svc = reader.ReadObject<IService>();

                    ResourceProcessor.Inject(svc, _ignite);

                    svc.Init(new ServiceContext(_ignite.Marshaller.StartUnmarshal(stream, srvKeepBinary)));

                    stream.Reset();

                    stream.WriteBool(true);  // Success.

                    stream.SynchronizeOutput();

                    return _handleRegistry.Allocate(svc);
                }
                catch (Exception e)
                {
                    stream.Reset();

                    var writer = _ignite.Marshaller.StartMarshal(stream);

                    BinaryUtils.WriteInvocationResult(writer, false, e);

                    _ignite.Marshaller.FinishMarshal(writer);

                    stream.SynchronizeOutput();

                    return 0;
                }
            }
        }

        private long ServiceExecute(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var svc = _handleRegistry.Get<IService>(stream.ReadLong());

                // Ignite does not guarantee that Cancel is called after Execute exits
                // So missing handle is a valid situation
                if (svc == null)
                    return 0;

                var reader = _ignite.Marshaller.StartUnmarshal(stream);

                bool srvKeepBinary = reader.ReadBoolean();

                svc.Execute(new ServiceContext(_ignite.Marshaller.StartUnmarshal(stream, srvKeepBinary)));

                return 0;
            }
        }

        private long ServiceCancel(long memPtr)
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

                    return 0;
                }
                finally
                {
                    _ignite.HandleRegistry.Release(svcPtr);
                }
            }
        }

        private long ServiceInvokeMethod(long memPtr)
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

                return 0;
            }
        }

        private long ClusterNodeFilterApply(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var reader = _ignite.Marshaller.StartUnmarshal(stream);

                var filter = reader.ReadObject<IClusterNodeFilter>();

                return filter.Invoke(_ignite.GetNode(reader.ReadGuid())) ? 1 : 0;
            }
        }

        #endregion

        #region IMPLEMENTATION: MISCELLANEOUS

        private long NodeInfo(long memPtr)
        {
            _ignite.UpdateNodeInfo(memPtr);

            return 0;
        }

        private static long MemoryReallocate(long memPtr, long cap, long unused, void* arg)
        {
            IgniteManager.Memory.Get(memPtr).Reallocate((int)cap);

            return 0;
        }

        private long OnStart(long memPtr, long unused, long unused1, void* proc)
        {
            var proc0 = _jvm.AttachCurrentThread().NewGlobalRef((IntPtr) proc);

            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                Ignition.OnStart(proc0, stream);
            }

            return 0;
        }

        private long OnStop(long unused)
        {
            // Notify grid
            var ignite = _ignite;

            if (ignite != null)
                ignite.AfterNodeStop();

            return 0;
        }

        private long OnClientDisconnected(long unused)
        {
            _ignite.OnClientDisconnected();

            return 0;
        }

        private long OnClientReconnected(long clusterRestarted)
        {
            _ignite.OnClientReconnected(clusterRestarted != 0);

            return 0;
        }

        internal void LoggerLog(int level, string message, string category, string nativeError, long memPtr)
        {
            // When custom logger in .NET is not defined, Java should not call us.
            Debug.Assert(!(_log is JavaLogger));

            SafeCall(() =>
            {
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

        internal bool LoggerIsLevelEnabled(int level)
        {
            // When custom logger in .NET is not defined, Java should not call us.
            Debug.Assert(!(_log is JavaLogger));

            return SafeCall(() => _log.IsEnabled((LogLevel) level), true);
        }

        private long PluginProcessorIgniteStop(long val)
        {
            _ignite.PluginProcessor.OnIgniteStop(val != 0);

            return 0;
        }

        private long PluginProcessorStop(long val)
        {
            _ignite.PluginProcessor.Stop(val != 0);

            return 0;
        }

        #endregion

        #region AffinityFunction

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private long AffinityFunctionInit(long memPtr, long unused, long unused1, void* baseFunc)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var reader = BinaryUtils.Marshaller.StartUnmarshal(stream);

                var func = reader.ReadObjectEx<IAffinityFunction>();

                ResourceProcessor.Inject(func, _ignite);

                var affBase = func as AffinityFunctionBase;

                if (affBase != null)
                {
                    var baseFuncRef = _jvm.AttachCurrentThread().NewGlobalRef((IntPtr) baseFunc);
                    var baseFunc0 = new PlatformJniTarget(baseFuncRef, _ignite.Marshaller);

                    affBase.SetBaseFunction(new PlatformAffinityFunction(baseFunc0));
                }

                return _handleRegistry.Allocate(func);
            }
        }

        private long AffinityFunctionPartition(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var ptr = stream.ReadLong();

                var key = _ignite.Marshaller.Unmarshal<object>(stream);

                return _handleRegistry.Get<IAffinityFunction>(ptr, true).GetPartition(key);
            }
        }

        private long AffinityFunctionAssignPartitions(long memPtr)
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

                return 0;
            }
        }

        private long AffinityFunctionRemoveNode(long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var ptr = stream.ReadLong();
                var nodeId = _ignite.Marshaller.Unmarshal<Guid>(stream);

                _handleRegistry.Get<IAffinityFunction>(ptr, true).RemoveNode(nodeId);

                return 0;
            }
        }

        private long AffinityFunctionDestroy(long ptr)
        {
            _handleRegistry.Release(ptr);

            return 0;
        }

        #endregion

        #region PLUGINS
  
        private long PluginCallbackInLongLongOutLong(long callbackId, long inPtr, long outPtr, void* arg)
        {
            return _ignite.PluginProcessor.InvokeCallback(callbackId, inPtr, outPtr);
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
                throw;
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
                throw;
            }
        }

        #endregion
        
        /// <summary>
        /// Gets the log.
        /// </summary>
        public ILogger Log
        {
            get { return _log; }
        }

        /// <summary>
        /// Sets the context.
        /// </summary>
        public void SetContext(long igniteId)
        {
            _igniteId = igniteId;
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
            // This instance crosses AppDomain boundaries and is GCed with a delay.
            // Release all external references.

            _ignite = null;
            _log = null;

            _jvm.ReleaseCallbacks(_igniteId);

            _handleRegistry.Close();
        }

        /// <summary>
        /// InLongOutLong handler struct.
        /// </summary>
        private struct InLongOutLongHandler
        {
            /// <summary> The handler func. </summary>
            public readonly InLongOutLongFunc Handler;

            /// <summary> Allow uninitialized flag. </summary>
            public readonly bool AllowUninitialized;

            /// <summary>
            /// Initializes a new instance of the <see cref="InLongOutLongHandler"/> struct.
            /// </summary>
            public InLongOutLongHandler(InLongOutLongFunc handler, bool allowUninitialized)
            {
                Handler = handler;
                AllowUninitialized = allowUninitialized;
            }
        }

        /// <summary>
        /// InLongLongLongObjectOutLong handler struct.
        /// </summary>
        private struct InLongLongLongObjectOutLongHandler
        {
            /// <summary> The handler func. </summary>
            public readonly InLongLongLongObjectOutLongFunc Handler;

            /// <summary> Allow uninitialized flag. </summary>
            public readonly bool AllowUninitialized;

            /// <summary>
            /// Initializes a new instance of the <see cref="InLongLongLongObjectOutLongHandler"/> struct.
            /// </summary>
            public InLongLongLongObjectOutLongHandler(InLongLongLongObjectOutLongFunc handler, bool allowUninitialized)
            {
                Handler = handler;
                AllowUninitialized = allowUninitialized;
            }
        }
    }
}
