﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
    public class IgniteExceptionTaskSelfTest : GridAbstractTaskTest
    {
        /** Error mode. */
        public static ErrorMode MODE;

        /** Observed job errors. */
        public static readonly ICollection<Exception> JOB_ERRS = new List<Exception>();

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
            MODE = ErrorMode.MAP_ERR;

            GoodException e = ExecuteWithError() as GoodException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.MAP_ERR, e.mode);
        }

        /// <summary>
        /// Test not-marshalable error occurred during map step.
        /// </summary>
        [Test]
        public void TestMapNotMarshalableError()
        {
            MODE = ErrorMode.MAP_ERR_NOT_MARSHALABLE;

            BadException e = ExecuteWithError() as BadException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.MAP_ERR_NOT_MARSHALABLE, e.mode);
        }

        /// <summary>
        /// Test task behavior when job produced by mapper is not marshalable.
        /// </summary>
        [Test]
        public void TestMapNotMarshalableJob()
        {
            MODE = ErrorMode.MAP_JOB_NOT_MARSHALABLE;

            SerializationException e = ExecuteWithError() as SerializationException;

            Assert.IsNotNull(e);
        }

        /// <summary>
        /// Test local job error.
        /// </summary>
        [Test]
        public void TestLocalJobError()
        {
            MODE = ErrorMode.LOC_JOB_ERR;

            int res = Execute();

            Assert.AreEqual(2, res);

            Assert.AreEqual(1, JOB_ERRS.Count);
            Assert.IsNotNull(JOB_ERRS.First() as GoodException);
            Assert.AreEqual(ErrorMode.LOC_JOB_ERR, (JOB_ERRS.First() as GoodException).mode);
        }

        /// <summary>
        /// Test local not-marshalable job error.
        /// </summary>
        [Test]
        public void TestLocalJobErrorNotMarshalable()
        {
            MODE = ErrorMode.LOC_JOB_ERR_NOT_MARSHALABLE;

            int res = Execute();

            Assert.AreEqual(2, res);

            Assert.AreEqual(1, JOB_ERRS.Count);
            Assert.IsNotNull(JOB_ERRS.First() as BadException); // Local job exception is not marshalled.
        }

        /// <summary>
        /// Test local not-marshalable job result.
        /// </summary>
        [Test]
        public void TestLocalJobResultNotMarshalable()
        {
            MODE = ErrorMode.LOC_JOB_RES_NOT_MARSHALABLE;

            int res = Execute();

            Assert.AreEqual(3, res); // Local job result is not marshalled.

            Assert.AreEqual(0, JOB_ERRS.Count);
        }

        /// <summary>
        /// Test remote job error.
        /// </summary>
        [Test]
        public void TestRemoteJobError()
        {
            MODE = ErrorMode.RMT_JOB_ERR;

            int res = Execute();

            Assert.AreEqual(1, res);

            Assert.AreEqual(2, JOB_ERRS.Count());

            Assert.IsNotNull(JOB_ERRS.ElementAt(0) as GoodException);
            Assert.IsNotNull(JOB_ERRS.ElementAt(1) as GoodException);

            Assert.AreEqual(ErrorMode.RMT_JOB_ERR, (JOB_ERRS.ElementAt(0) as GoodException).mode);
            Assert.AreEqual(ErrorMode.RMT_JOB_ERR, (JOB_ERRS.ElementAt(1) as GoodException).mode);
        }

        /// <summary>
        /// Test remote not-marshalable job error.
        /// </summary>
        [Test]
        public void TestRemoteJobErrorNotMarshalable()
        {
            MODE = ErrorMode.RMT_JOB_ERR_NOT_MARSHALABLE;

            int res = Execute();

            Assert.AreEqual(1, res);

            Assert.AreEqual(2, JOB_ERRS.Count());

            Assert.IsNotNull(JOB_ERRS.ElementAt(0) as IgniteException);
            Assert.IsNotNull(JOB_ERRS.ElementAt(1) as IgniteException);
        }

        /// <summary>
        /// Test local not-marshalable job result.
        /// </summary>
        [Test]
        public void TestRemoteJobResultNotMarshalable()
        {
            MODE = ErrorMode.RMT_JOB_RES_NOT_MARSHALABLE;

            int res = Execute();

            Assert.AreEqual(1, res);

            Assert.AreEqual(2, JOB_ERRS.Count);

            Assert.IsNotNull(JOB_ERRS.ElementAt(0) as IgniteException);
            Assert.IsNotNull(JOB_ERRS.ElementAt(1) as IgniteException);
        }

        /// <summary>
        /// Test local result error.
        /// </summary>
        [Test]
        public void TestLocalResultError()
        {
            MODE = ErrorMode.LOC_RES_ERR;

            GoodException e = ExecuteWithError() as GoodException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.LOC_RES_ERR, e.mode);
        }

        /// <summary>
        /// Test local result not-marshalable error.
        /// </summary>
        [Test]
        public void TestLocalResultErrorNotMarshalable()
        {
            MODE = ErrorMode.LOC_RES_ERR_NOT_MARSHALABLE;

            BadException e = ExecuteWithError() as BadException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.LOC_RES_ERR_NOT_MARSHALABLE, e.mode);
        }

        /// <summary>
        /// Test remote result error.
        /// </summary>
        [Test]
        public void TestRemoteResultError()
        {
            MODE = ErrorMode.RMT_RES_ERR;

            GoodException e = ExecuteWithError() as GoodException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.RMT_RES_ERR, e.mode);
        }

        /// <summary>
        /// Test remote result not-marshalable error.
        /// </summary>
        [Test]
        public void TestRemoteResultErrorNotMarshalable()
        {
            MODE = ErrorMode.RMT_RES_ERR_NOT_MARSHALABLE;

            BadException e = ExecuteWithError() as BadException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.RMT_RES_ERR_NOT_MARSHALABLE, e.mode);
        }

        /// <summary>
        /// Test reduce with error.
        /// </summary>
        [Test]
        public void TestReduceError()
        {
            MODE = ErrorMode.REDUCE_ERR;

            GoodException e = ExecuteWithError() as GoodException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.REDUCE_ERR, e.mode);
        }

        /// <summary>
        /// Test reduce with not-marshalable error.
        /// </summary>
        [Test]
        public void TestReduceErrorNotMarshalable()
        {
            MODE = ErrorMode.REDUCE_ERR_NOT_MARSHALABLE;

            BadException e = ExecuteWithError() as BadException;

            Assert.IsNotNull(e);

            Assert.AreEqual(ErrorMode.REDUCE_ERR_NOT_MARSHALABLE, e.mode);
        }

        /// <summary>
        /// Test reduce with not-marshalable result.
        /// </summary>
        [Test]
        public void TestReduceResultNotMarshalable()
        {
            MODE = ErrorMode.REDUCE_RES_NOT_MARSHALABLE;

            int res = Execute();

            Assert.AreEqual(3, res);
        }

        /// <summary>
        /// Execute task successfully.
        /// </summary>
        /// <returns>Task result.</returns>
        private int Execute()
        {
            JOB_ERRS.Clear();

            object res = grid1.Compute().Execute(new Task());

            return res is GoodTaskResult ? ((GoodTaskResult)res).res : ((BadTaskResult)res).res;
        }

        /// <summary>
        /// Execute task with error.
        /// </summary>
        /// <returns>Task</returns>
        private Exception ExecuteWithError()
        {
            JOB_ERRS.Clear();

            Exception err = null;

            try
            {
                grid1.Compute().Execute(new Task());

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
            MAP_ERR,

            /** Error during map step which is not marshalable. */
            MAP_ERR_NOT_MARSHALABLE,

            /** Job created by mapper is not marshalable. */
            MAP_JOB_NOT_MARSHALABLE,

            /** Error occurred in local job. */
            LOC_JOB_ERR,

            /** Error occurred in local job and is not marshalable. */
            LOC_JOB_ERR_NOT_MARSHALABLE,

            /** Local job result is not marshalable. */
            LOC_JOB_RES_NOT_MARSHALABLE,

            /** Error occurred in remote job. */
            RMT_JOB_ERR,

            /** Error occurred in remote job and is not marshalable. */
            RMT_JOB_ERR_NOT_MARSHALABLE,

            /** Remote job result is not marshalable. */
            RMT_JOB_RES_NOT_MARSHALABLE,            

            /** Error occurred during local result processing. */
            LOC_RES_ERR,

            /** Error occurred during local result processing and is not marshalable. */
            LOC_RES_ERR_NOT_MARSHALABLE,

            /** Error occurred during remote result processing. */
            RMT_RES_ERR,

            /** Error occurred during remote result processing and is not marshalable. */
            RMT_RES_ERR_NOT_MARSHALABLE,

            /** Error during reduce step. */
            REDUCE_ERR,

            /** Error during reduce step and is not marshalable. */
            REDUCE_ERR_NOT_MARSHALABLE,

            /** Reduce result is not marshalable. */
            REDUCE_RES_NOT_MARSHALABLE
        }

        /// <summary>
        /// Task.
        /// </summary>
        public class Task : IComputeTask<object, object>
        {
            /** Grid. */
            [InstanceResource]
            private IIgnite grid = null;

            /** Result. */
            private int res;

            /** <inheritDoc /> */
            public IDictionary<IComputeJob<object>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
            {
                switch (MODE)
                {
                    case ErrorMode.MAP_ERR:
                        throw new GoodException(ErrorMode.MAP_ERR);

                    case ErrorMode.MAP_ERR_NOT_MARSHALABLE:
                        throw new BadException(ErrorMode.MAP_ERR_NOT_MARSHALABLE);

                    case ErrorMode.MAP_JOB_NOT_MARSHALABLE:
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
                    jobs.Add(new GoodJob(!grid.Cluster.LocalNode.Id.Equals(node.Id)), node);

                return jobs;
            }

            /** <inheritDoc /> */
            public ComputeJobResultPolicy Result(IComputeJobResult<object> res, IList<IComputeJobResult<object>> rcvd)
            {
                if (res.Exception() != null)
                    JOB_ERRS.Add(res.Exception());
                else
                {
                    object res0 = res.Data();

                    bool rmt = res0 is GoodJobResult ? ((GoodJobResult)res0).rmt : ((BadJobResult)res0).rmt;

                    if (rmt)
                    {
                        switch (MODE)
                        {
                            case ErrorMode.RMT_RES_ERR:
                                throw new GoodException(ErrorMode.RMT_RES_ERR);

                            case ErrorMode.RMT_RES_ERR_NOT_MARSHALABLE:
                                throw new BadException(ErrorMode.RMT_RES_ERR_NOT_MARSHALABLE);
                        }
                    }
                    else
                    {
                        switch (MODE)
                        {
                            case ErrorMode.LOC_RES_ERR:
                                throw new GoodException(ErrorMode.LOC_RES_ERR);

                            case ErrorMode.LOC_RES_ERR_NOT_MARSHALABLE:
                                throw new BadException(ErrorMode.LOC_RES_ERR_NOT_MARSHALABLE);
                        }
                    }

                    this.res += 1;
                }

                return ComputeJobResultPolicy.WAIT;
            }

            /** <inheritDoc /> */
            public object Reduce(IList<IComputeJobResult<object>> results)
            {
                switch (MODE)
                {
                    case ErrorMode.REDUCE_ERR:
                        throw new GoodException(ErrorMode.REDUCE_ERR);

                    case ErrorMode.REDUCE_ERR_NOT_MARSHALABLE:
                        throw new BadException(ErrorMode.REDUCE_ERR_NOT_MARSHALABLE);

                    case ErrorMode.REDUCE_RES_NOT_MARSHALABLE:
                        return new BadTaskResult(res);
                }

                return new GoodTaskResult(res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [Serializable]
        public class GoodJob : IComputeJob<object>
        {
            /** Whether the job is remote. */
            private bool rmt;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="rmt"></param>
            public GoodJob(bool rmt)
            {
                this.rmt = rmt;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public GoodJob(SerializationInfo info, StreamingContext context)
            {
                rmt = info.GetBoolean("rmt");
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("rmt", rmt);
            }

            /** <inheritDoc /> */
            public object Execute()
            {
                if (rmt)
                {
                    switch (MODE)
                    {
                        case ErrorMode.RMT_JOB_ERR:
                            throw new GoodException(ErrorMode.RMT_JOB_ERR);

                        case ErrorMode.RMT_JOB_ERR_NOT_MARSHALABLE:
                            throw new BadException(ErrorMode.RMT_JOB_ERR);

                        case ErrorMode.RMT_JOB_RES_NOT_MARSHALABLE:
                            return new BadJobResult(rmt);
                    }
                }
                else
                {
                    switch (MODE)
                    {
                        case ErrorMode.LOC_JOB_ERR:
                            throw new GoodException(ErrorMode.LOC_JOB_ERR);

                        case ErrorMode.LOC_JOB_ERR_NOT_MARSHALABLE:
                            throw new BadException(ErrorMode.LOC_JOB_ERR);

                        case ErrorMode.LOC_JOB_RES_NOT_MARSHALABLE:
                            return new BadJobResult(rmt);
                    }
                }

                return new GoodJobResult(rmt);
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
            public bool rmt;
            
            /// <summary>
            /// 
            /// </summary>
            /// <param name="rmt"></param>
            public GoodJobResult(bool rmt)
            {
                this.rmt = rmt;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public GoodJobResult(SerializationInfo info, StreamingContext context)
            {
                rmt = info.GetBoolean("rmt");
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("rmt", rmt);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public class BadJobResult
        {
            /** */
            public bool rmt;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="rmt"></param>
            public BadJobResult(bool rmt)
            {
                this.rmt = rmt;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        [Serializable]
        public class GoodTaskResult
        {
            /** */
            public int res;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="res"></param>
            public GoodTaskResult(int res)
            {
                this.res = res;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public GoodTaskResult(SerializationInfo info, StreamingContext context)
            {
                res = info.GetInt32("res");
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("res", res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public class BadTaskResult
        {
            /** */
            public int res;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="res"></param>
            public BadTaskResult(int res)
            {
                this.res = res;
            }
        }

        /// <summary>
        /// Marshalable exception.
        /// </summary>
        [Serializable]
        public class GoodException : Exception
        {
            /** */
            public ErrorMode mode;
            
            /// <summary>
            /// 
            /// </summary>
            /// <param name="mode"></param>
            public GoodException(ErrorMode mode)
            {
                this.mode = mode;
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public GoodException(SerializationInfo info, StreamingContext context)
            {
                mode = (ErrorMode)info.GetInt32("mode");
            }

            /** <inheritDoc /> */
            public override void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("mode", (int)mode);

                base.GetObjectData(info, context);
            }
        }

        /// <summary>
        /// Not marshalable exception.
        /// </summary>
        public class BadException : Exception
        {
            /** */
            public ErrorMode mode;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="mode"></param>
            public BadException(ErrorMode mode)
            {
                this.mode = mode;
            }
        }
    }
}
