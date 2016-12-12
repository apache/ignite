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

namespace Apache.Ignite.Core.Impl.Compute
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Compute.Closure;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Compute implementation.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class ComputeImpl : PlatformTarget
    {
        /** */
        private const int OpAffinity = 1;

        /** */
        private const int OpBroadcast = 2;

        /** */
        private const int OpExec = 3;

        /** */
        private const int OpExecAsync = 4;

        /** */
        private const int OpUnicast = 5;

        /** */
        private const int OpWithNoFailover = 6;

        /** */
        private const int OpWithTimeout = 7;

        /** */
        private const int OpExecNative = 8;

        /** Underlying projection. */
        private readonly ClusterGroupImpl _prj;

        /** Whether objects must be kept in binary form. */
        private readonly ThreadLocal<bool> _keepBinary = new ThreadLocal<bool>(() => false);

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="prj">Projection.</param>
        /// <param name="keepBinary">Binary flag.</param>
        public ComputeImpl(IUnmanagedTarget target, Marshaller marsh, ClusterGroupImpl prj, bool keepBinary)
            : base(target, marsh)
        {
            _prj = prj;

            _keepBinary.Value = keepBinary;
        }

        /// <summary>
        /// Grid projection to which this compute instance belongs.
        /// </summary>
        public IClusterGroup ClusterGroup
        {
            get
            {
                return _prj;
            }
        }

        /// <summary>
        /// Sets no-failover flag for the next executed task on this projection in the current thread.
        /// If flag is set, job will be never failed over even if remote node crashes or rejects execution.
        /// When task starts execution, the no-failover flag is reset, so all other task will use default
        /// failover policy, unless this flag is set again.
        /// </summary>
        public void WithNoFailover()
        {
            DoOutInOp(OpWithNoFailover);
        }

        /// <summary>
        /// Sets task timeout for the next executed task on this projection in the current thread.
        /// When task starts execution, the timeout is reset, so one timeout is used only once.
        /// </summary>
        /// <param name="timeout">Computation timeout in milliseconds.</param>
        public void WithTimeout(long timeout)
        {
            DoOutInOp(OpWithTimeout, timeout);
        }

        /// <summary>
        /// Sets keep-binary flag for the next executed Java task on this projection in the current
        /// thread so that task argument passed to Java and returned task results will not be
        /// deserialized.
        /// </summary>
        public void WithKeepBinary()
        {
            _keepBinary.Value = true;
        }

        /// <summary>
        /// Executes given Java task on the grid projection. If task for given name has not been deployed yet,
        /// then 'taskName' will be used as task class name to auto-deploy the task.
        /// </summary>
        public TReduceRes ExecuteJavaTask<TReduceRes>(string taskName, object taskArg)
        {
            IgniteArgumentCheck.NotNullOrEmpty(taskName, "taskName");

            ICollection<IClusterNode> nodes = _prj.Predicate == null ? null : _prj.GetNodes();

            try
            {
                return DoOutInOp<TReduceRes>(OpExec, writer => WriteTask(writer, taskName, taskArg, nodes));
            }
            finally
            {
                _keepBinary.Value = false;
            }
        }

        /// <summary>
        /// Executes given Java task asynchronously on the grid projection.
        /// If task for given name has not been deployed yet,
        /// then 'taskName' will be used as task class name to auto-deploy the task.
        /// </summary>
        public Future<TReduceRes> ExecuteJavaTaskAsync<TReduceRes>(string taskName, object taskArg)
        {
            IgniteArgumentCheck.NotNullOrEmpty(taskName, "taskName");

            ICollection<IClusterNode> nodes = _prj.Predicate == null ? null : _prj.GetNodes();

            try
            {
                return DoOutOpObjectAsync<TReduceRes>(OpExecAsync, w => WriteTask(w, taskName, taskArg, nodes));
            }
            finally
            {
                _keepBinary.Value = false;
            }
        }

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="task">Task to execute.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <returns>Task result.</returns>
        public Future<TReduceRes> Execute<TArg, TJobRes, TReduceRes>(IComputeTask<TArg, TJobRes, TReduceRes> task, 
            TArg taskArg)
        {
            IgniteArgumentCheck.NotNull(task, "task");

            var holder = new ComputeTaskHolder<TArg, TJobRes, TReduceRes>((Ignite) _prj.Ignite, this, task, taskArg);

            long ptr = Marshaller.Ignite.HandleRegistry.Allocate(holder);

            var futTarget = DoOutOpObject(OpExecNative, w =>
            {
                w.WriteLong(ptr);
                w.WriteLong(_prj.TopologyVersion);
            });

            var future = holder.Future;

            future.SetTarget(new Listenable(futTarget, Marshaller));

            return future;
        }

        /// <summary>
        /// Executes given task on the grid projection. For step-by-step explanation of task execution process
        /// refer to <see cref="IComputeTask{A,T,R}"/> documentation.
        /// </summary>
        /// <param name="taskType">Task type.</param>
        /// <param name="taskArg">Optional task argument.</param>
        /// <returns>Task result.</returns>
        public Future<TReduceRes> Execute<TArg, TJobRes, TReduceRes>(Type taskType, TArg taskArg)
        {
            IgniteArgumentCheck.NotNull(taskType, "taskType");

            object task = FormatterServices.GetUninitializedObject(taskType);

            var task0 = task as IComputeTask<TArg, TJobRes, TReduceRes>;

            if (task0 == null)
                throw new IgniteException("Task type doesn't implement IComputeTask: " + taskType.Name);

            return Execute(task0, taskArg);
        }

        /// <summary>
        /// Executes provided job on a node in this grid projection. The result of the
        /// job execution is returned from the result closure.
        /// </summary>
        /// <param name="clo">Job to execute.</param>
        /// <returns>Job result for this execution.</returns>
        public Future<TJobRes> Execute<TJobRes>(IComputeFunc<TJobRes> clo)
        {
            IgniteArgumentCheck.NotNull(clo, "clo");

            return ExecuteClosures0(new ComputeSingleClosureTask<object, TJobRes, TJobRes>(),
                new ComputeOutFuncJob(clo.ToNonGeneric()), null, false);
        }

        /// <summary>
        /// Executes provided delegate on a node in this grid projection. The result of the
        /// job execution is returned from the result closure.
        /// </summary>
        /// <param name="func">Func to execute.</param>
        /// <returns>Job result for this execution.</returns>
        public Future<TJobRes> Execute<TJobRes>(Func<TJobRes> func)
        {
            IgniteArgumentCheck.NotNull(func, "func");

            var wrappedFunc = new ComputeOutFuncWrapper(func, () => func());

            return ExecuteClosures0(new ComputeSingleClosureTask<object, TJobRes, TJobRes>(),
                new ComputeOutFuncJob(wrappedFunc), null, false);
        }

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <returns>Collection of job results for this execution.</returns>
        public Future<ICollection<TJobRes>> Execute<TJobRes>(IEnumerable<IComputeFunc<TJobRes>> clos)
        {
            IgniteArgumentCheck.NotNull(clos, "clos");

            ICollection<IComputeJob> jobs = new List<IComputeJob>(GetCountOrZero(clos));

            foreach (IComputeFunc<TJobRes> clo in clos)
                jobs.Add(new ComputeOutFuncJob(clo.ToNonGeneric()));

            return ExecuteClosures0(new ComputeMultiClosureTask<object, TJobRes, ICollection<TJobRes>>(jobs.Count),
                null, jobs, false);
        }

        /// <summary>
        /// Executes collection of jobs on nodes within this grid projection.
        /// </summary>
        /// <param name="clos">Collection of jobs to execute.</param>
        /// <param name="rdc">Reducer to reduce all job results into one individual return value.</param>
        /// <returns>Collection of job results for this execution.</returns>
        public Future<TReduceRes> Execute<TJobRes, TReduceRes>(IEnumerable<IComputeFunc<TJobRes>> clos, 
            IComputeReducer<TJobRes, TReduceRes> rdc)
        {
            IgniteArgumentCheck.NotNull(clos, "clos");

            ICollection<IComputeJob> jobs = new List<IComputeJob>(GetCountOrZero(clos));

            foreach (var clo in clos)
                jobs.Add(new ComputeOutFuncJob(clo.ToNonGeneric()));

            return ExecuteClosures0(new ComputeReducingClosureTask<object, TJobRes, TReduceRes>(rdc), null, jobs, false);
        }

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection. Every participating node will return a job result.
        /// </summary>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <returns>Collection of results for this execution.</returns>
        public Future<ICollection<TJobRes>> Broadcast<TJobRes>(IComputeFunc<TJobRes> clo)
        {
            IgniteArgumentCheck.NotNull(clo, "clo");

            return ExecuteClosures0(new ComputeMultiClosureTask<object, TJobRes, ICollection<TJobRes>>(1),
                new ComputeOutFuncJob(clo.ToNonGeneric()), null, true);
        }

        /// <summary>
        /// Broadcasts given closure job with passed in argument to all nodes in grid projection.
        /// Every participating node will return a job result.
        /// </summary>
        /// <param name="clo">Job to broadcast to all projection nodes.</param>
        /// <param name="arg">Job closure argument.</param>
        /// <returns>Collection of results for this execution.</returns>
        public Future<ICollection<TJobRes>> Broadcast<TArg, TJobRes>(IComputeFunc<TArg, TJobRes> clo, TArg arg)
        {
            IgniteArgumentCheck.NotNull(clo, "clo");

            return ExecuteClosures0(new ComputeMultiClosureTask<object, TJobRes, ICollection<TJobRes>>(1),
                new ComputeFuncJob(clo.ToNonGeneric(), arg), null, true);
        }

        /// <summary>
        /// Broadcasts given job to all nodes in grid projection.
        /// </summary>
        /// <param name="action">Job to broadcast to all projection nodes.</param>
        public Future<object> Broadcast(IComputeAction action)
        {
            IgniteArgumentCheck.NotNull(action, "action");

            return ExecuteClosures0(new ComputeSingleClosureTask<object, object, object>(),
                new ComputeActionJob(action), opId: OpBroadcast);
        }

        /// <summary>
        /// Executes provided job on a node in this grid projection.
        /// </summary>
        /// <param name="action">Job to execute.</param>
        public Future<object> Run(IComputeAction action)
        {
            IgniteArgumentCheck.NotNull(action, "action");

            return ExecuteClosures0(new ComputeSingleClosureTask<object, object, object>(),
                new ComputeActionJob(action));
        }

        /// <summary>
        /// Executes collection of jobs on Ignite nodes within this grid projection.
        /// </summary>
        /// <param name="actions">Jobs to execute.</param>
        public Future<object> Run(IEnumerable<IComputeAction> actions)
        {
            IgniteArgumentCheck.NotNull(actions, "actions");

            var actions0 = actions as ICollection;

            if (actions0 == null)
            {
                var jobs = actions.Select(a => new ComputeActionJob(a)).ToList();

                return ExecuteClosures0(new ComputeSingleClosureTask<object, object, object>(), jobs: jobs,
                    jobsCount: jobs.Count);
            }
            else
            {
                var jobs = actions.Select(a => new ComputeActionJob(a));

                return ExecuteClosures0(new ComputeSingleClosureTask<object, object, object>(), jobs: jobs,
                    jobsCount: actions0.Count);
            }
        }

        /// <summary>
        /// Executes provided closure job on a node in this grid projection.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="arg">Job argument.</param>
        /// <returns>Job result for this execution.</returns>
        public Future<TJobRes> Apply<TArg, TJobRes>(IComputeFunc<TArg, TJobRes> clo, TArg arg)
        {
            IgniteArgumentCheck.NotNull(clo, "clo");

            return ExecuteClosures0(new ComputeSingleClosureTask<TArg, TJobRes, TJobRes>(),
                new ComputeFuncJob(clo.ToNonGeneric(), arg), null, false);
        }

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <returns>Collection of job results.</returns>
        public Future<ICollection<TJobRes>> Apply<TArg, TJobRes>(IComputeFunc<TArg, TJobRes> clo, 
            IEnumerable<TArg> args)
        {
            IgniteArgumentCheck.NotNull(clo, "clo");

            IgniteArgumentCheck.NotNull(clo, "clo");

            var jobs = new List<IComputeJob>(GetCountOrZero(args));

            var func = clo.ToNonGeneric();
            
            foreach (TArg arg in args)
                jobs.Add(new ComputeFuncJob(func, arg));

            return ExecuteClosures0(new ComputeMultiClosureTask<TArg, TJobRes, ICollection<TJobRes>>(jobs.Count),
                null, jobs, false);
        }

        /// <summary>
        /// Executes provided closure job on nodes within this grid projection. A new job is executed for
        /// every argument in the passed in collection. The number of actual job executions will be
        /// equal to size of the job arguments collection. The returned job results will be reduced
        /// into an individual result by provided reducer.
        /// </summary>
        /// <param name="clo">Job to run.</param>
        /// <param name="args">Job arguments.</param>
        /// <param name="rdc">Reducer to reduce all job results into one individual return value.</param>
        /// <returns>Reduced job result for this execution.</returns>
        public Future<TReduceRes> Apply<TArg, TJobRes, TReduceRes>(IComputeFunc<TArg, TJobRes> clo, 
            IEnumerable<TArg> args, IComputeReducer<TJobRes, TReduceRes> rdc)
        {
            IgniteArgumentCheck.NotNull(clo, "clo");

            IgniteArgumentCheck.NotNull(clo, "clo");

            IgniteArgumentCheck.NotNull(clo, "clo");

            ICollection<IComputeJob> jobs = new List<IComputeJob>(GetCountOrZero(args));

            var func = clo.ToNonGeneric();

            foreach (TArg arg in args)
                jobs.Add(new ComputeFuncJob(func, arg));

            return ExecuteClosures0(new ComputeReducingClosureTask<TArg, TJobRes, TReduceRes>(rdc),
                null, jobs, false);
        }

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="action">Job to execute.</param>
        public Future<object> AffinityRun(string cacheName, object affinityKey, IComputeAction action)
        {
            IgniteArgumentCheck.NotNull(action, "action");

            return ExecuteClosures0(new ComputeSingleClosureTask<object, object, object>(),
                new ComputeActionJob(action), opId: OpAffinity,
                writeAction: w => WriteAffinity(w, cacheName, affinityKey));
        }

        /// <summary>
        /// Executes given job on the node where data for provided affinity key is located
        /// (a.k.a. affinity co-location).
        /// </summary>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        /// <param name="clo">Job to execute.</param>
        /// <returns>Job result for this execution.</returns>
        /// <typeparam name="TJobRes">Type of job result.</typeparam>
        public Future<TJobRes> AffinityCall<TJobRes>(string cacheName, object affinityKey, IComputeFunc<TJobRes> clo)
        {
            IgniteArgumentCheck.NotNull(clo, "clo");

            return ExecuteClosures0(new ComputeSingleClosureTask<object, TJobRes, TJobRes>(),
                new ComputeOutFuncJob(clo.ToNonGeneric()), opId: OpAffinity,
                writeAction: w => WriteAffinity(w, cacheName, affinityKey));
        }

        /** <inheritDoc /> */
        protected override T Unmarshal<T>(IBinaryStream stream)
        {
            bool keep = _keepBinary.Value;

            return Marshaller.Unmarshal<T>(stream, keep);
        }

        /// <summary>
        /// Internal routine for closure-based task execution.
        /// </summary>
        /// <param name="task">Task.</param>
        /// <param name="job">Job.</param>
        /// <param name="jobs">Jobs.</param>
        /// <param name="broadcast">Broadcast flag.</param>
        /// <returns>Future.</returns>
        private Future<TReduceRes> ExecuteClosures0<TArg, TJobRes, TReduceRes>(
            IComputeTask<TArg, TJobRes, TReduceRes> task, IComputeJob job,
            ICollection<IComputeJob> jobs, bool broadcast)
        {
            return ExecuteClosures0(task, job, jobs, broadcast ? OpBroadcast : OpUnicast,
                jobs == null ? 1 : jobs.Count);
        }

        /// <summary>
        /// Internal routine for closure-based task execution.
        /// </summary>
        /// <param name="task">Task.</param>
        /// <param name="job">Job.</param>
        /// <param name="jobs">Jobs.</param>
        /// <param name="opId">Op code.</param>
        /// <param name="jobsCount">Jobs count.</param>
        /// <param name="writeAction">Custom write action.</param>
        /// <returns>Future.</returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "User code can throw any exception")]
        private Future<TReduceRes> ExecuteClosures0<TArg, TJobRes, TReduceRes>(
            IComputeTask<TArg, TJobRes, TReduceRes> task, IComputeJob job = null,
            IEnumerable<IComputeJob> jobs = null, int opId = OpUnicast, int jobsCount = 0,
            Action<BinaryWriter> writeAction = null)
        {
            Debug.Assert(job != null || jobs != null);

            var holder = new ComputeTaskHolder<TArg, TJobRes, TReduceRes>((Ignite) _prj.Ignite, this, task, default(TArg));

            var taskHandle = Marshaller.Ignite.HandleRegistry.Allocate(holder);

            var jobHandles = new List<long>(job != null ? 1 : jobsCount);

            try
            {
                Exception err = null;

                try
                {
                    var futTarget = DoOutOpObject(opId, writer =>
                    {
                        writer.WriteLong(taskHandle);

                        if (job != null)
                        {
                            writer.WriteInt(1);

                            jobHandles.Add(WriteJob(job, writer));
                        }
                        else
                        {
                            writer.WriteInt(jobsCount);

                            Debug.Assert(jobs != null, "jobs != null");

                            jobHandles.AddRange(jobs.Select(jobEntry => WriteJob(jobEntry, writer)));
                        }
                        
                        holder.JobHandles(jobHandles);

                        if (writeAction != null)
                            writeAction(writer);
                    });

                    holder.Future.SetTarget(new Listenable(futTarget, Marshaller));
                }
                catch (Exception e)
                {
                    err = e;
                }

                if (err != null)
                {
                    // Manual job handles release because they were not assigned to the task yet.
                    foreach (var hnd in jobHandles) 
                        Marshaller.Ignite.HandleRegistry.Release(hnd);

                    holder.CompleteWithError(taskHandle, err);
                }
            }
            catch (Exception e)
            {
                // This exception means that out-op failed.
                holder.CompleteWithError(taskHandle, e);
            }

            return holder.Future;
        }

        /// <summary>
        /// Writes the job.
        /// </summary>
        /// <param name="job">The job.</param>
        /// <param name="writer">The writer.</param>
        /// <returns>Handle to the job holder</returns>
        private long WriteJob(IComputeJob job, BinaryWriter writer)
        {
            var jobHolder = new ComputeJobHolder((Ignite) _prj.Ignite, job);

            var jobHandle = Marshaller.Ignite.HandleRegistry.Allocate(jobHolder);

            writer.WriteLong(jobHandle);

            try
            {
                writer.WriteObject(jobHolder);
            }
            catch (Exception)
            {
                Marshaller.Ignite.HandleRegistry.Release(jobHandle);

                throw;
            }

            return jobHandle;
        }

        /// <summary>
        /// Write task to the writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="taskName">Task name.</param>
        /// <param name="taskArg">Task arg.</param>
        /// <param name="nodes">Nodes.</param>
        private void WriteTask(IBinaryRawWriter writer, string taskName, object taskArg,
            ICollection<IClusterNode> nodes)
        {
            writer.WriteString(taskName);
            writer.WriteBoolean(_keepBinary.Value);
            writer.WriteObject(taskArg);

            WriteNodeIds(writer, nodes);
        }

        /// <summary>
        /// Write node IDs.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="nodes">Nodes.</param>
        private static void WriteNodeIds(IBinaryRawWriter writer, ICollection<IClusterNode> nodes)
        {
            if (nodes == null)
                writer.WriteBoolean(false);
            else
            {
                writer.WriteBoolean(true);
                writer.WriteInt(nodes.Count);

                foreach (IClusterNode node in nodes)
                    writer.WriteGuid(node.Id);
            }
        }

        /// <summary>
        /// Writes the affinity info.
        /// </summary>
        /// <param name="writer">The writer.</param>
        /// <param name="cacheName">Name of the cache to use for affinity co-location.</param>
        /// <param name="affinityKey">Affinity key.</param>
        private static void WriteAffinity(BinaryWriter writer, string cacheName, object affinityKey)
        {
            writer.WriteString(cacheName);

            writer.WriteObject(affinityKey);
        }

        /// <summary>
        /// Gets element count or zero.
        /// </summary>
        private static int GetCountOrZero(object collection)
        {
            var coll = collection as ICollection;

            return coll == null ? 0 : coll.Count;
        }
    }
}
