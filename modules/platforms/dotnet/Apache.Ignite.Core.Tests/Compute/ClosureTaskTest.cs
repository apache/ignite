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
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests for distributed closure executions.
    /// </summary>
    public abstract class ClosureTaskTest : AbstractTaskTest
    {
        /** Amount of multiple closures. */
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
            CheckResult(Grid1.GetCompute().Call(OutFunc(false)));
            CheckResult(Grid1.GetCompute().CallAsync(OutFunc(false)).Result);
        }

        /// <summary>
        /// Test for single closure returning exception.
        /// </summary>
        [Test]
        public void TestExecuteSingleException()
        {
            CheckError(Assert.Catch(() => Grid1.GetCompute().Call(OutFunc(true))));
            CheckError(Assert.Catch(() => Grid1.GetCompute().CallAsync(OutFunc(true)).Wait()));
        }

        /// <summary>
        /// Test for multiple closures execution.
        /// </summary>
        [Test]
        public void TestExecuteMultiple()
        {
            var clos = Enumerable.Range(0, MultiCloCnt).Select(x => OutFunc(false)).ToArray();

            Grid1.GetCompute().Call(clos).ToList().ForEach(CheckResult);
            Grid1.GetCompute().CallAsync(clos).Result.ToList().ForEach(CheckResult);
        }

        /// <summary>
        /// Test for multiple closures execution.
        /// </summary>
        [Test]
        public void TestExecuteMultipleReduced()
        {
            var clos = Enumerable.Range(0, MultiCloCnt).Select(x => OutFunc(false)).ToArray();

            Grid1.GetCompute().Call(clos, new Reducer(false)).ToList().ForEach(CheckResult);
            Grid1.GetCompute().CallAsync(clos, new Reducer(false)).Result.ToList().ForEach(CheckResult);
        }

        /// <summary>
        /// Test for multiple closures execution with exceptions thrown from some of them.
        /// </summary>
        [Test]
        public void TestExecuteMultipleException()
        {
            // Some closures will be faulty.
            var clos = Enumerable.Range(0, MultiCloCnt).Select(x => OutFunc(x % 2 == 0)).ToArray();

            CheckError(Assert.Catch(() => Grid1.GetCompute().Call(clos)));
            CheckError(Assert.Catch(() => Grid1.GetCompute().CallAsync(clos).Wait()));
        }

        /// <summary>
        /// Test broadcast out-closure execution.
        /// </summary>
        [Test]
        public void TestBroadcastOut()
        {
            Grid1.GetCompute().Broadcast(OutFunc(false)).ToList().ForEach(CheckResult);
            Grid1.GetCompute().BroadcastAsync(OutFunc(false)).Result.ToList().ForEach(CheckResult);
        }

        /// <summary>
        /// Test broadcast out-closure execution with exception.
        /// </summary>
        [Test]
        public void TestBroadcastOutException()
        {
            CheckError(Assert.Catch(() => Grid1.GetCompute().Broadcast(OutFunc(true))));
            CheckError(Assert.Catch(() => Grid1.GetCompute().BroadcastAsync(OutFunc(true)).Wait()));
        }

        /// <summary>
        /// Test broadcast in-out-closure execution.
        /// </summary>
        [Test]
        public void TestBroadcastInOut()
        {
            Grid1.GetCompute().Broadcast(Func(false), 1).ToList().ForEach(CheckResult);
            Grid1.GetCompute().BroadcastAsync(Func(false), 1).Result.ToList().ForEach(CheckResult);
        }

        /// <summary>
        /// Test broadcast in-out-closure execution with exception.
        /// </summary>
        [Test]
        public void TestBroadcastInOutException()
        {
            CheckError(Assert.Catch(() => Grid1.GetCompute().Broadcast(Func(true), 1)));
            CheckError(Assert.Catch(() => Grid1.GetCompute().BroadcastAsync(Func(true), 1).Wait()));
        }

        /// <summary>
        /// Test apply in-out-closure execution.
        /// </summary>
        [Test]
        public void TestApply()
        {
            CheckResult(Grid1.GetCompute().Apply(Func(false), 1));
            CheckResult(Grid1.GetCompute().ApplyAsync(Func(false), 1).Result);
        }

        /// <summary>
        /// Test apply in-out-closure execution with exception.
        /// </summary>
        [Test]
        public void TestApplyException()
        {
            CheckError(Assert.Catch(() => Grid1.GetCompute().Apply(Func(true), 1)));
            CheckError(Assert.Catch(() => Grid1.GetCompute().ApplyAsync(Func(true), 1).Wait()));
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution.
        /// </summary>
        [Test]
        public void TestApplyMultiple()
        {
            var args = Enumerable.Repeat(1, MultiCloCnt).Cast<object>().ToArray();

            Grid1.GetCompute().Apply(Func(false), args).ToList().ForEach(CheckResult);
            Grid1.GetCompute().ApplyAsync(Func(false), args).Result.ToList().ForEach(CheckResult);
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution with exception.
        /// </summary>
        [Test]
        public void TestApplyMultipleException()
        {
            var args = Enumerable.Repeat(1, MultiCloCnt).Cast<object>().ToArray();

            CheckError(Assert.Catch(() => Grid1.GetCompute().Apply(Func(true), args)));
            CheckError(Assert.Catch(() => Grid1.GetCompute().ApplyAsync(Func(true), args).Wait()));
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution with reducer.
        /// </summary>
        [Test]
        public void TestApplyMultipleReducer()
        {
            var args = Enumerable.Repeat(1, MultiCloCnt).Cast<object>().ToArray();

            Grid1.GetCompute().Apply(Func(false), args, new Reducer(false)).ToList().ForEach(CheckResult);
            Grid1.GetCompute().ApplyAsync(Func(false), args, new Reducer(false)).Result.ToList().ForEach(CheckResult);
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution with reducer and exception thrown from closure.
        /// </summary>
        [Test]
        public void TestAppylMultipleReducerJobException()
        {
            var args = Enumerable.Repeat(1, MultiCloCnt).Cast<object>().ToArray();

            CheckError(Assert.Catch(() => Grid1.GetCompute().Apply(Func(true), args, new Reducer(false))));
            CheckError(Assert.Catch(() => Grid1.GetCompute().ApplyAsync(Func(true), args, new Reducer(false)).Wait()));
        }

        /// <summary>
        /// Test apply multiple in-out-closures execution with reducer and exception thrown from reducer.
        /// </summary>
        [Test]
        public void TestAppylMultipleReducerReduceException()
        {
            var args = Enumerable.Repeat(1, MultiCloCnt).Cast<object>().ToArray();

            var e = Assert.Throws<AggregateException>(() =>
                Grid1.GetCompute().Apply(Func(false), args, new Reducer(true)));

            Assert.IsNotNull(e.InnerException);
            Assert.AreEqual(ErrMsg, e.InnerException.Message);
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
