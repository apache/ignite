/*
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
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Closure execution tests for serializable objects.
    /// </summary>
    public class GridSerializableClosureTaskTest : GridClosureTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public GridSerializableClosureTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected GridSerializableClosureTaskTest(bool fork) : base(fork) { }

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
            Assert.AreEqual(1, res0.res);
        }

        /** <inheritDoc /> */
        protected override void CheckError(Exception err)
        {
            Assert.IsTrue(err != null);

            SerializableException err0 = err as SerializableException;

            Assert.IsTrue(err0 != null);
            Assert.AreEqual(ERR_MSG, err0.msg);
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        private class SerializableOutFunc : IComputeFunc<object>
        {
            /** Error. */
            private bool err;

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
                this.err = err;
            }

            /** <inheritDoc /> */
            public object Invoke()
            {
                if (err)
                    throw new SerializableException(ERR_MSG);
                else
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
            private bool err;

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
                this.err = err;
            }

            /** <inheritDoc /> */
            public object Invoke(object arg)
            {
                Console.WriteLine("INVOKED!");

                if (err)
                    throw new SerializableException(ERR_MSG);
                else
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
            public string msg;

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
                this.msg = msg;
            }
            /// <summary>
            ///
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public SerializableException(SerializationInfo info, StreamingContext context) : base(info, context)
            {
                msg = info.GetString("msg");
            }

            /** <inheritDoc /> */
            public override void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("msg", msg);

                base.GetObjectData(info, context);
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        private class SerializableResult
        {
            public int res;

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
                this.res = res;
            }
        }
    }
}
