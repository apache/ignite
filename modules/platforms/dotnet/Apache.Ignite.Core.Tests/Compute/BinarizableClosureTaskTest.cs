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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Closure execution tests for binary objects.
    /// </summary>
    [TestFixture]
    public class BinarizableClosureTaskTest : ClosureTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public BinarizableClosureTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected BinarizableClosureTaskTest(bool fork) : base(fork) { }

        /** <inheritDoc /> */
        protected override IComputeFunc<object> OutFunc(bool err)
        {
            return new BinarizableOutFunc(err);
        }

        /** <inheritDoc /> */
        protected override IComputeFunc<object, object> Func(bool err)
        {
            return new BinarizableFunc(err);
        }

        /** <inheritDoc /> */
        protected override void CheckResult(object res)
        {
            Assert.IsTrue(res != null);

            var res0 = res as BinarizableResult;

            Assert.IsNotNull(res0);
            Assert.AreEqual(1, res0.Res);
        }

        /** <inheritDoc /> */
        protected override void CheckError(Exception err)
        {
            Assert.IsTrue(err != null);

            err = err.InnerException;
            Assert.IsNotNull(err);

            var err0 = err.InnerException as BinarizableException;

            Assert.IsNotNull(err0);
            Assert.AreEqual(ErrMsg, err0.Msg);
        }

        /// <summary>
        /// 
        /// </summary>
        private class BinarizableOutFunc : IComputeFunc<object>
        {
            /** Error. */
            private readonly bool _err;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="err"></param>
            public BinarizableOutFunc(bool err)
            {
                _err = err;
            }
            
            /** <inheritDoc /> */
            public object Invoke()
            {
                if (_err)
                    throw new BinarizableException(ErrMsg);
                return new BinarizableResult(1);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private class BinarizableFunc : IComputeFunc<object, object>
        {
            /** Error. */
            private readonly bool _err;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="err"></param>
            public BinarizableFunc(bool err)
            {
                _err = err;
            }
            
            /** <inheritDoc /> */
            public object Invoke(object arg)
            {
                if (_err)
                    throw new BinarizableException(ErrMsg);
                return new BinarizableResult(1);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private class BinarizableException : Exception, IBinarizable
        {
            /** */
            public string Msg;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="msg"></param>
            public BinarizableException(string msg)
            {
                Msg = msg;
            }

            /** <inheritDoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.GetRawWriter().WriteString(Msg);
            }

            /** <inheritDoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                Msg = reader.GetRawReader().ReadString();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private class BinarizableResult
        {
            /** */
            public readonly int Res;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="res"></param>
            public BinarizableResult(int res)
            {
                Res = res;
            }
        }
    }
}
