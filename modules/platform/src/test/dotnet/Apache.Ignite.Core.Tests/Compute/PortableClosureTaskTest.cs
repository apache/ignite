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
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    /// Closure execution tests for portable objects.
    /// </summary>
    public class PortableClosureTaskTest : ClosureTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public PortableClosureTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected PortableClosureTaskTest(bool fork) : base(fork) { }

        /** <inheritDoc /> */
        protected override void PortableTypeConfigurations(ICollection<PortableTypeConfiguration> portTypeCfgs)
        {
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableOutFunc)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableFunc)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableResult)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableException)));
        }

        /** <inheritDoc /> */
        protected override IComputeFunc<object> OutFunc(bool err)
        {
            return new PortableOutFunc(err);
        }

        /** <inheritDoc /> */
        protected override IComputeFunc<object, object> Func(bool err)
        {
            return new PortableFunc(err);
        }

        /** <inheritDoc /> */
        protected override void CheckResult(object res)
        {
            Assert.IsTrue(res != null);

            PortableResult res0 = res as PortableResult;

            Assert.IsTrue(res0 != null);
            Assert.AreEqual(1, res0.Res);
        }

        /** <inheritDoc /> */
        protected override void CheckError(Exception err)
        {
            Assert.IsTrue(err != null);

            PortableException err0 = err as PortableException;

            Assert.IsTrue(err0 != null);
            Assert.AreEqual(ErrMsg, err0.Msg);
        }

        /// <summary>
        /// 
        /// </summary>
        private class PortableOutFunc : IComputeFunc<object>
        {
            /** Error. */
            private bool _err;

            /// <summary>
            /// 
            /// </summary>
            public PortableOutFunc()
            {
                // No-op.
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="err"></param>
            public PortableOutFunc(bool err)
            {
                _err = err;
            }
            
            /** <inheritDoc /> */
            public object Invoke()
            {
                if (_err)
                    throw new PortableException(ErrMsg);
                return new PortableResult(1);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private class PortableFunc : IComputeFunc<object, object>
        {
            /** Error. */
            private bool _err;

            /// <summary>
            /// 
            /// </summary>
            public PortableFunc()
            {
                // No-op.
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="err"></param>
            public PortableFunc(bool err)
            {
                _err = err;
            }
            
            /** <inheritDoc /> */
            public object Invoke(object arg)
            {
                if (_err)
                    throw new PortableException(ErrMsg);
                return new PortableResult(1);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private class PortableException : Exception, IPortableMarshalAware
        {
            /** */
            public string Msg;

            /// <summary>
            /// 
            /// </summary>
            public PortableException()
            {
                // No-op.
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="msg"></param>
            public PortableException(string msg) : this()
            {
                Msg = msg;
            }

            /** <inheritDoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.RawWriter().WriteString(Msg);
            }

            /** <inheritDoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                Msg = reader.RawReader().ReadString();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private class PortableResult
        {
            /** */
            public int Res;

            /// <summary>
            /// 
            /// </summary>
            public PortableResult()
            {
                // No-op.
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="res"></param>
            public PortableResult(int res)
            {
                Res = res;
            }
        }
    }
}
