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

namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests for exception handling on various task execution stages.
    /// </summary>
    public class IgniteExceptionTaskSelfTest : AbstractTaskTest
    {
        /** Error mode. */
        public static ErrorMode Mode;

        /** Observed job errors. */
        public static readonly ICollection<Exception> JobErrs = new List<Exception>();

        /// <summary>
        /// Constructor.
        /// </summary>
        public IgniteExceptionTaskSelfTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected IgniteExceptionTaskSelfTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test error occurred during map step.
        /// </summary>
        [Test]
        public void TestMapError()
        {
            Mode = ErrorMode.MapErr;

            GoodException e = ExecuteWithError() as GoodException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.MapErr, e.Mode);
        }

        /// <summary>
        /// Test not-marshalable error occurred during map step.
        /// </summary>
        [Test]
        public void TestMapNotMarshalableError()
        {
            Mode = ErrorMode.MapErrNotMarshalable;

            BadException e = ExecuteWithError() as BadException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.MapErrNotMarshalable, e.Mode);
        }

        /// <summary>
        /// Test task behavior when job produced by mapper is not marshalable.
        /// </summary>
        [Test]
        public void TestMapNotMarshalableJob()
        {
            Mode = ErrorMode.MapJobNotMarshalable;

            SerializationException e = ExecuteWithError() as SerializationException;

            Assert.IsNotNull(e);
        }

        /// <summary>
        /// Test local job error.
        /// </summary>
        [Test]
        public void TestLocalJobError()
        {
            Mode = ErrorMode.LocJobErr;

            int res = Execute();

            Assert.AreEqual(2, res);

            Assert.AreEqual(1, JobErrs.Count);
            Assert.IsNotNull(JobErrs.First() as GoodException);
            Assert.AreEqual(ErrorMode.LocJobErr, ((GoodException) JobErrs.First()).Mode);
        }

        /// <summary>
        /// Test local not-marshalable job error.
        /// </summary>
        [Test]
        public void TestLocalJobErrorNotMarshalable()
        {
            Mode = ErrorMode.LocJobErrNotMarshalable;

            int res = Execute();

            Assert.AreEqual(2, res);

            Assert.AreEqual(1, JobErrs.Count);
            Assert.IsNotNull(JobErrs.First() as BadException); // Local job exception is not marshalled.
        }

        /// <summary>
        /// Test local not-marshalable job result.
        /// </summary>
        [Test]
        public void TestLocalJobResultNotMarshalable()
        {
            Mode = ErrorMode.LocJobResNotMarshalable;

            int res = Execute();

            Assert.AreEqual(3, res); // Local job result is not marshalled.

            Assert.AreEqual(0, JobErrs.Count);
        }

        /// <summary>
        /// Test remote job error.
        /// </summary>
        [Test]
        public void TestRemoteJobError()
        {
            Mode = ErrorMode.RmtJobErr;

            int res = Execute();

            Assert.AreEqual(1, res);

            Assert.AreEqual(2, JobErrs.Count);

            Assert.IsNotNull(JobErrs.ElementAt(0) as GoodException);
            Assert.IsNotNull(JobErrs.ElementAt(1) as GoodException);

            Assert.AreEqual(ErrorMode.RmtJobErr, ((GoodException) JobErrs.ElementAt(0)).Mode);
            Assert.AreEqual(ErrorMode.RmtJobErr, ((GoodException) JobErrs.ElementAt(1)).Mode);
        }

        /// <summary>
        /// Test remote not-marshalable job error.
        /// </summary>
        [Test]
        public void TestRemoteJobErrorNotMarshalable()
        {
            Mode = ErrorMode.RmtJobErrNotMarshalable;

            int res = Execute();

            Assert.AreEqual(1, res);

            Assert.AreEqual(2, JobErrs.Count);

            Assert.IsNotNull(JobErrs.ElementAt(0) as IgniteException);
            Assert.IsNotNull(JobErrs.ElementAt(1) as IgniteException);
        }

        /// <summary>
        /// Test local not-marshalable job result.
        /// </summary>
        [Test]
        public void TestRemoteJobResultNotMarshalable()
        {
            Mode = ErrorMode.RmtJobResNotMarshalable;

            int res = Execute();

            Assert.AreEqual(1, res);

            Assert.AreEqual(2, JobErrs.Count);

            Assert.IsNotNull(JobErrs.ElementAt(0) as IgniteException);
            Assert.IsNotNull(JobErrs.ElementAt(1) as IgniteException);
        }

        /// <summary>
        /// Test local result error.
        /// </summary>
        [Test]
        public void TestLocalResultError()
        {
            Mode = ErrorMode.LocResErr;

            GoodException e = ExecuteWithError() as GoodException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.LocResErr, e.Mode);
        }

        /// <summary>
        /// Test local result not-marshalable error.
        /// </summary>
        [Test]
        public void TestLocalResultErrorNotMarshalable()
        {
            Mode = ErrorMode.LocResErrNotMarshalable;

            BadException e = ExecuteWithError() as BadException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.LocResErrNotMarshalable, e.Mode);
        }

        /// <summary>
        /// Test remote result error.
        /// </summary>
        [Test]
        public void TestRemoteResultError()
        {
            Mode = ErrorMode.RmtResErr;

            GoodException e = ExecuteWithError() as GoodException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.RmtResErr, e.Mode);
        }

        /// <summary>
        /// Test remote result not-marshalable error.
        /// </summary>
        [Test]
        public void TestRemoteResultErrorNotMarshalable()
        {
            Mode = ErrorMode.RmtResErrNotMarshalable;

            BadException e = ExecuteWithError() as BadException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.RmtResErrNotMarshalable, e.Mode);
        }

        /// <summary>
        /// Test reduce with error.
        /// </summary>
        [Test]
        public void TestReduceError()
        {
            Mode = ErrorMode.ReduceErr;

            GoodException e = ExecuteWithError() as GoodException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.ReduceErr, e.Mode);
        }

        /// <summary>
        /// Test reduce with not-marshalable error.
        /// </summary>
        [Test]
        public void TestReduceErrorNotMarshalable()
        {
            Mode = ErrorMode.ReduceErrNotMarshalable;

            BadException e = ExecuteWithError() as BadException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.ReduceErrNotMarshalable, e.Mode);
        }

        /// <summary>
        /// Test reduce with not-marshalable result.
        /// </summary>
        [Test]
        public void TestReduceResultNotMarshalable()
        {
            Mode = ErrorMode.ReduceResNotMarshalable;

            int res = Execute();

            Assert.AreEqual(3, res);
        }

        /// <summary>
        /// Execute task successfully.
        /// </summary>
        /// <returns>Task result.</returns>
        private int Execute()
        {
            JobErrs.Clear();

            object res = Grid1.Compute().Execute(new Task());

            return res is GoodTaskResult ? ((GoodTaskResult)res).Res : ((BadTaskResult)res).Res;
        }

        /// <summary>
        /// Execute task with error.
        /// </summary>
        /// <returns>Task</returns>
        private Exception ExecuteWithError()
        {
            JobErrs.Clear();

            Exception err = null;

            try
            {
                Grid1.Compute().Execute(new Task());

                Assert.Fail();
            }
            catch (Exception e)
            {
                err = e;
            }

            return err;
        }

        /// <summary>
        /// Error modes.
        /// </summary>
        public enum ErrorMode
        {
            /** Error during map step. */
            MapErr,

            /** Error during map step which is not marshalable. */
            MapErrNotMarshalable,

            /** Job created by mapper is not marshalable. */
            MapJobNotMarshalable,

            /** Error occurred in local job. */
            LocJobErr,

            /** Error occurred in local job and is not marshalable. */
            LocJobErrNotMarshalable,

            /** Local job result is not marshalable. */
            LocJobResNotMarshalable,

            /** Error occurred in remote job. */
            RmtJobErr,

            /** Error occurred in remote job and is not marshalable. */
            RmtJobErrNotMarshalable,

            /** Remote job result is not marshalable. */
            RmtJobResNotMarshalable,            

            /** Error occurred during local result processing. */
            LocResErr,

            /** Error occurred during local result processing and is not marshalable. */
            LocResErrNotMarshalable,

            /** Error occurred during remote result processing. */
            RmtResErr,

            /** Error occurred during remote result processing and is not marshalable. */
            RmtResErrNotMarshalable,

            /** Error during reduce step. */
            ReduceErr,

            /** Error during reduce step and is not marshalable. */
            ReduceErrNotMarshalable,

            /** Reduce result is not marshalable. */
            ReduceResNotMarshalable
        }

        /// <summary>
        /// Task.
        /// </summary>
        public class Task : IComputeTask<object, object>
        {
            /** Grid. */
            [InstanceResource]
            private IIgnite _grid = null;

            /** Result. */
            private int _res;

            /** <inheritDoc /> */
            public IDictionary<IComputeJob<object>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
            {
                switch (Mode)
                {
                    case ErrorMode.MapErr:
                        throw new GoodException(ErrorMode.MapErr);

                    case ErrorMode.MapErrNotMarshalable:
                        throw new BadException(ErrorMode.MapErrNotMarshalable);

                    case ErrorMode.MapJobNotMarshalable:
                    {
                        var badJobs = new Dictionary<IComputeJob<object>, IClusterNode>();

                        foreach (IClusterNode node in subgrid)
                            badJobs.Add(new BadJob(), node);

                        return badJobs;
                    }
                }

                // Map completes sucessfully and we spread jobs to all nodes.
                var jobs = new Dictionary<IComputeJob<object>, IClusterNode>();

                foreach (IClusterNode node in subgrid)
                    jobs.Add(new GoodJob(!_grid.Cluster.LocalNode.Id.Equals(node.Id)), node);

                return jobs;
            }

            /** <inheritDoc /> */
            public ComputeJobResultPolicy Result(IComputeJobResult<object> res, IList<IComputeJobResult<object>> rcvd)
            {
                if (res.Exception() != null)
                    JobErrs.Add(res.Exception());
                else
                {
                    object res0 = res.Data();

                    bool rmt = res0 is GoodJobResult ? ((GoodJobResult)res0).Rmt : ((BadJobResult)res0).Rmt;

                    if (rmt)
                    {
                        switch (Mode)
                        {
                            case ErrorMode.RmtResErr:
                                throw new GoodException(ErrorMode.RmtResErr);

                            case ErrorMode.RmtResErrNotMarshalable:
                                throw new BadException(ErrorMode.RmtResErrNotMarshalable);
                        }
                    }
                    else
                    {
                        switch (Mode)
                        {
                            case ErrorMode.LocResErr:
                                throw new GoodException(ErrorMode.LocResErr);

                            case ErrorMode.LocResErrNotMarshalable:
                                throw new BadException(ErrorMode.LocResErrNotMarshalable);
                        }
                    }

                    _res += 1;
                }

                return ComputeJobResultPolicy.Wait;
            }

            /** <inheritDoc /> */
            public object Reduce(IList<IComputeJobResult<object>> results)
            {
                switch (Mode)
                {
                    case ErrorMode.ReduceErr:
                        throw new GoodException(ErrorMode.ReduceErr);

                    case ErrorMode.ReduceErrNotMarshalable:
                        throw new BadException(ErrorMode.ReduceErrNotMarshalable);

                    case ErrorMode.ReduceResNotMarshalable:
                        return new BadTaskResult(_res);
                }

                return new GoodTaskResult(_res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [Serializable]
        public class GoodJob : IComputeJob<object>
        {
            /** Whether the job is remote. */
            private bool _rmt;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="rmt"></param>
            public GoodJob(bool rmt)
            {
                _rmt = rmt;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public GoodJob(SerializationInfo info, StreamingContext context)
            {
                _rmt = info.GetBoolean("rmt");
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("rmt", _rmt);
            }

            /** <inheritDoc /> */
            public object Execute()
            {
                if (_rmt)
                {
                    switch (Mode)
                    {
                        case ErrorMode.RmtJobErr:
                            throw new GoodException(ErrorMode.RmtJobErr);

                        case ErrorMode.RmtJobErrNotMarshalable:
                            throw new BadException(ErrorMode.RmtJobErr);

                        case ErrorMode.RmtJobResNotMarshalable:
                            return new BadJobResult(_rmt);
                    }
                }
                else
                {
                    switch (Mode)
                    {
                        case ErrorMode.LocJobErr:
                            throw new GoodException(ErrorMode.LocJobErr);

                        case ErrorMode.LocJobErrNotMarshalable:
                            throw new BadException(ErrorMode.LocJobErr);

                        case ErrorMode.LocJobResNotMarshalable:
                            return new BadJobResult(_rmt);
                    }
                }

                return new GoodJobResult(_rmt);
            }

            /** <inheritDoc /> */
            public void Cancel()
            {
                // No-op.
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public class BadJob : IComputeJob<object>
        {
            [InstanceResource]

            /** <inheritDoc /> */
            public object Execute()
            {
                throw new NotImplementedException();
            }

            /** <inheritDoc /> */
            public void Cancel()
            {
                // No-op.
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [Serializable]
        public class GoodJobResult
        {
            /** */
            public bool Rmt;
            
            /// <summary>
            /// 
            /// </summary>
            /// <param name="rmt"></param>
            public GoodJobResult(bool rmt)
            {
                Rmt = rmt;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public GoodJobResult(SerializationInfo info, StreamingContext context)
            {
                Rmt = info.GetBoolean("rmt");
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("rmt", Rmt);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public class BadJobResult
        {
            /** */
            public bool Rmt;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="rmt"></param>
            public BadJobResult(bool rmt)
            {
                Rmt = rmt;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [Serializable]
        public class GoodTaskResult
        {
            /** */
            public int Res;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="res"></param>
            public GoodTaskResult(int res)
            {
                Res = res;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public GoodTaskResult(SerializationInfo info, StreamingContext context)
            {
                Res = info.GetInt32("res");
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("res", Res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public class BadTaskResult
        {
            /** */
            public int Res;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="res"></param>
            public BadTaskResult(int res)
            {
                Res = res;
            }
        }

        /// <summary>
        /// Marshalable exception.
        /// </summary>
        [Serializable]
        public class GoodException : Exception
        {
            /** */
            public ErrorMode Mode;
            
            /// <summary>
            /// 
            /// </summary>
            /// <param name="mode"></param>
            public GoodException(ErrorMode mode)
            {
                Mode = mode;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public GoodException(SerializationInfo info, StreamingContext context)
            {
                Mode = (ErrorMode)info.GetInt32("mode");
            }

            /** <inheritDoc /> */
            public override void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("mode", (int)Mode);

                base.GetObjectData(info, context);
            }
        }

        /// <summary>
        /// Not marshalable exception.
        /// </summary>
        public class BadException : Exception
        {
            /** */
            public ErrorMode Mode;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="mode"></param>
            public BadException(ErrorMode mode)
            {
                Mode = mode;
            }
        }
    }
}
