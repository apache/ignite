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
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Closure execution tests for serializable objects.
    /// </summary>
    public class SerializableClosureTaskTest : ClosureTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public SerializableClosureTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected SerializableClosureTaskTest(bool fork) : base(fork) { }

        /** <inheritDoc /> */
        protected override IComputeFunc<object> OutFunc(bool err)
        {
            return new SerializableOutFunc(err);
        }

        /** <inheritDoc /> */
        protected override IComputeFunc<object, object> Func(bool err)
        {
            return new SerializableFunc(err);
        }

        /** <inheritDoc /> */
        protected override void CheckResult(object res)
        {
            Assert.IsTrue(res != null);

            SerializableResult res0 = res as SerializableResult;

            Assert.IsTrue(res0 != null);
            Assert.AreEqual(1, res0.Res);
        }

        /** <inheritDoc /> */
        protected override void CheckError(Exception err)
        {
            Assert.IsTrue(err != null);

            SerializableException err0 = err as SerializableException;

            Assert.IsTrue(err0 != null);
            Assert.AreEqual(ErrMsg, err0.Msg);
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        private class SerializableOutFunc : IComputeFunc<object>
        {
            /** Error. */
            private bool _err;

            /// <summary>
            ///
            /// </summary>
            public SerializableOutFunc()
            {
                // No-op.
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="err"></param>
            public SerializableOutFunc(bool err)
            {
                _err = err;
            }

            /** <inheritDoc /> */
            public object Invoke()
            {
                if (_err)
                    throw new SerializableException(ErrMsg);
                return new SerializableResult(1);
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        private class SerializableFunc : IComputeFunc<object, object>
        {
            /** Error. */
            private bool _err;

            /// <summary>
            ///
            /// </summary>
            public SerializableFunc()
            {
                // No-op.
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="err"></param>
            public SerializableFunc(bool err)
            {
                _err = err;
            }

            /** <inheritDoc /> */
            public object Invoke(object arg)
            {
                Console.WriteLine("INVOKED!");

                if (_err)
                    throw new SerializableException(ErrMsg);
                return new SerializableResult(1);
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        private class SerializableException : Exception
        {
            /** */
            public string Msg;

            /// <summary>
            ///
            /// </summary>
            public SerializableException()
            {
                // No-op.
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="msg"></param>
            public SerializableException(string msg) : this()
            {
                Msg = msg;
            }
            /// <summary>
            ///
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public SerializableException(SerializationInfo info, StreamingContext context) : base(info, context)
            {
                Msg = info.GetString("msg");
            }

            /** <inheritDoc /> */
            public override void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("msg", Msg);

                base.GetObjectData(info, context);
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        private class SerializableResult
        {
            public int Res;

            /// <summary>
            ///
            /// </summary>
            public SerializableResult()
            {
                // No-op.
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="res"></param>
            public SerializableResult(int res)
            {
                Res = res;
            }
        }
    }
}
