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
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests for distributed closure executions.
    /// </summary>
    public abstract class ClosureTaskTest : AbstractTaskTest
    {
        /** Amount of multiple clousres. */
        private const int MultiCloCnt = 5;

        /** */
        protected const string ErrMsg = "An error has occurred.";

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork mode.</param>
        protected ClosureTaskTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test for single closure returning result.
        /// </summary>
        [Test]
        public void TestExecuteSingle()
        {
            var res = Grid1.Compute().Call(OutFunc(false));

            CheckResult(res);
        }

        /// <summary>
        /// Test for single closure returning exception.
        /// </summary>
        [Test]
        public void TestExecuteSingleException()
        {
            try
            {
                Grid1.Compute().Call(OutFunc(true));

                Assert.Fail();
            }
            catch (Exception e)
            {
                CheckError(e);
            }
        }

        /// <summary>
        /// Test for multiple closures execution.
        /// </summary>
        [Test]
        public void TestExecuteMultiple()
        {
            var clos = new List<IComputeFunc<object>>(MultiCloCnt);

            for (int i = 0; i < MultiCloCnt; i++)
                clos.Add(OutFunc(false));

            ICollection<object> ress = Grid1.Compute().Call(clos);

            foreach (object res in ress)
                CheckResult(res);
        }

        /// <summary>
        /// Test for multiple closures execution.
        /// </summary>
        [Test]
        public void TestExecuteMultipleReduced()
        {
            var clos = new List<IComputeFunc<object>>(MultiCloCnt);

            for (int i = 0; i < MultiCloCnt; i++)
                clos.Add(OutFunc(false));

            ICollection<object> ress = Grid1.Compute().Call(clos, new Reducer(false));

            foreach (object res in ress)
                CheckResult(res);
        }

        /// <summary>
        /// Test for multiple closures execution with exceptions thrown from some of them.
        /// </summary>
        [Test]
        public void TestExecuteMultipleException()
        {
            var clos = new List<IComputeFunc<object>>(MultiCloCnt);

            for (int i = 0; i < MultiCloCnt; i++)
                clos.Add(OutFunc(i % 2 == 0)); // Some closures will be faulty.

            try
            {
                Grid1.Compute().Call(clos);

                Assert.Fail();
            }
            catch (Exception e)
            {
                CheckError(e);
            }
        }

        /// <summary>
        /// Test broadcast out-closure execution.
        /// </summary>
        [Test]
        public void TestBroadcastOut()
        {
            ICollection<object> ress = Grid1.Compute().Broadcast(OutFunc(false));

            foreach (object res in ress)
                CheckResult(res);
        }

        /// <summary>
        /// Test broadcast out-closure execution with exception.
        /// </summary>
        [Test]
        public void TestBroadcastOutException()
        {
            try
            {
                Grid1.Compute().Broadcast(OutFunc(true));

                Assert.Fail();
            }
            catch (Exception e)
            {
                CheckError(e);
            }
        }

        /// <summary>
        /// Test broadcast in-out-closure execution.
        /// </summary>
        [Test]
        public void TestBroadcastInOut()
        {
            ICollection<object> ress = Grid1.Compute().Broadcast(Func(false), 1);

            foreach (object res in ress)
                CheckResult(res);
        }

        /// <summary>
        /// Test broadcast in-out-closure execution with exception.
        /// </summary>
        [Test]
        public void TestBroadcastInOutException()
        {
            try
            {
                Grid1.Compute().Broadcast(Func(true), 1);

                Assert.Fail();
            }
            catch (Exception e)
            {
                CheckError(e);
            }
        }

        /// <summary>
        /// Test apply in-out-closure execution.
        /// </summary>
        [Test]
        public void TestApply()
        {
            object res = Grid1.Compute().Apply(Func(false), 1);

            CheckResult(res);
        }

        /// <summary>
        /// Test apply in-out-closure execution with exception.
        /// </summary>
        [Test]
        public void TestApplyException()
        {
            try
            {
                Grid1.Compute().Apply(Func(true), 1);

                Assert.Fail();
            }
            catch (Exception e)
            {
                CheckError(e);
            }
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution.
        /// </summary>
        [Test]
        public void TestApplyMultiple()
        {
            var args = new List<object>(MultiCloCnt);

            for (int i = 0; i < MultiCloCnt; i++)
                args.Add(1);

            Console.WriteLine("START TASK");

            var ress = Grid1.Compute().Apply(Func(false), args);

            Console.WriteLine("END TASK.");

            foreach (object res in ress)
                CheckResult(res);
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution with exception.
        /// </summary>
        [Test]
        public void TestApplyMultipleException()
        {
            ICollection<int> args = new List<int>(MultiCloCnt);

            for (int i = 0; i < MultiCloCnt; i++)
                args.Add(1);

            try
            {
                Grid1.Compute().Apply(Func(true), args);

                Assert.Fail();
            }
            catch (Exception e)
            {
                CheckError(e);
            }
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution with reducer.
        /// </summary>
        [Test]
        public void TestApplyMultipleReducer()
        {
            var args = new List<object>(MultiCloCnt);

            for (int i = 0; i < MultiCloCnt; i++)
                args.Add(1);

            ICollection<object> ress =
                Grid1.Compute().Apply(Func(false), args, new Reducer(false));

            foreach (object res in ress)
                CheckResult(res);
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution with reducer and exception thrown from closure.
        /// </summary>
        [Test]
        public void TestAppylMultipleReducerJobException()
        {
            List<object> args = new List<object>(MultiCloCnt);

            for (int i = 0; i < MultiCloCnt; i++)
                args.Add(1);

            try
            {
                Grid1.Compute().Apply(Func(true), args, new Reducer(false));

                Assert.Fail();
            }
            catch (Exception e)
            {
                CheckError(e);
            }
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution with reducer and exception thrown from reducer.
        /// </summary>
        [Test]
        public void TestAppylMultipleReducerReduceException()
        {
            var args = new List<object>(MultiCloCnt);

            for (int i = 0; i < MultiCloCnt; i++)
                args.Add(1);

            try
            {
                Grid1.Compute().Apply(Func(false), args, new Reducer(true));

                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.AreEqual(typeof(Exception), e.GetType());

                Assert.AreEqual(ErrMsg, e.Message);
            }
        }

        /// <summary>
        /// Create out-only closure.
        /// </summary>
        /// <param name="err">Error flag.</param>
        /// <returns>Closure.</returns>
        protected abstract IComputeFunc<object> OutFunc(bool err);

        /// <summary>
        /// Create in-out closure.
        /// </summary>
        /// <param name="err">Error flag.</param>
        /// <returns>Closure.</returns>
        protected abstract IComputeFunc<object, object> Func(bool err);

        /// <summary>
        /// Check result.
        /// </summary>
        /// <param name="res">Result.</param>
        protected abstract void CheckResult(object res);

        /// <summary>
        /// Check error.
        /// </summary>
        /// <param name="err">Error.</param>
        protected abstract void CheckError(Exception err);

        /// <summary>
        /// Test reducer.
        /// </summary>
        public class Reducer : IComputeReducer<object, ICollection<object>>
        {
            /** Whether to throw an error on reduce. */
            private readonly bool _err;

            /** Results. */
            private readonly ICollection<object> _ress = new List<object>();

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="err">Error.</param>
            public Reducer(bool err)
            {
                _err = err;
            }

            /** <inheritDoc /> */
            public bool Collect(object res)
            {
                _ress.Add(res);

                return true;
            }

            /** <inheritDoc /> */
            public ICollection<object> Reduce()
            {
                if (_err)
                    throw new Exception(ErrMsg);
                return _ress;
            }
        }
    }
}
