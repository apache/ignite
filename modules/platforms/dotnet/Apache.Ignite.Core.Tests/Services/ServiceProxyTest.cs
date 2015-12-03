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

namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Services;
    using Apache.Ignite.Core.Services;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="ServiceProxySerializer"/> functionality.
    /// </summary>
    public class ServiceProxyTest
    {
        /** */
        private TestIgniteService _svc;

        /** */
        private readonly Marshaller _marsh = new Marshaller(new BinaryConfiguration
        {
            TypeConfigurations = new[]
            {
                new BinaryTypeConfiguration(typeof (TestBinarizableClass)),
                new BinaryTypeConfiguration(typeof (CustomExceptionBinarizable))
            }
        });

        /** */
        protected readonly IBinary Binary;

        /** */
        private readonly PlatformMemoryManager _memory = new PlatformMemoryManager(1024);

        /** */
        protected bool KeepBinary;

        /** */
        protected bool SrvKeepBinary;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceProxyTest"/> class.
        /// </summary>
        public ServiceProxyTest()
        {
            Binary = new Binary(_marsh);
        }

        /// <summary>
        /// Tests object class methods proxying.
        /// </summary>
        [Test]
        public void TestObjectClassMethods()
        {
            var prx = GetProxy();

            prx.IntProp = 12345;

            Assert.AreEqual("12345", prx.ToString());
            Assert.AreEqual("12345", _svc.ToString());
            Assert.AreEqual(12345, prx.GetHashCode());
            Assert.AreEqual(12345, _svc.GetHashCode());
        }

        /// <summary>
        /// Tests properties proxying.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        public void TestProperties()
        {
            var prx = GetProxy();

            prx.IntProp = 10;
            Assert.AreEqual(10, prx.IntProp);
            Assert.AreEqual(10, _svc.IntProp);

            _svc.IntProp = 15;
            Assert.AreEqual(15, prx.IntProp);
            Assert.AreEqual(15, _svc.IntProp);

            prx.ObjProp = "prop1";
            Assert.AreEqual("prop1", prx.ObjProp);
            Assert.AreEqual("prop1", _svc.ObjProp);

            prx.ObjProp = null;
            Assert.IsNull(prx.ObjProp);
            Assert.IsNull(_svc.ObjProp);

            prx.ObjProp = new TestClass {Prop = "prop2"};
            Assert.AreEqual("prop2", ((TestClass)prx.ObjProp).Prop);
            Assert.AreEqual("prop2", ((TestClass)_svc.ObjProp).Prop);
        }

        /// <summary>
        /// Tests void methods proxying.
        /// </summary>
        [Test]
        public void TestVoidMethods()
        {
            var prx = GetProxy();

            prx.VoidMethod();
            Assert.AreEqual("VoidMethod", prx.InvokeResult);
            Assert.AreEqual("VoidMethod", _svc.InvokeResult);

            prx.VoidMethod(10);
            Assert.AreEqual(_svc.InvokeResult, prx.InvokeResult);

            prx.VoidMethod(10, "string");
            Assert.AreEqual(_svc.InvokeResult, prx.InvokeResult);

            prx.VoidMethod(10, "string", "arg");
            Assert.AreEqual(_svc.InvokeResult, prx.InvokeResult);

            prx.VoidMethod(10, "string", "arg", "arg1", 2, 3, "arg4");
            Assert.AreEqual(_svc.InvokeResult, prx.InvokeResult);
        }

        /// <summary>
        /// Tests object methods proxying.
        /// </summary>
        [Test]
        public void TestObjectMethods()
        {
            var prx = GetProxy();

            Assert.AreEqual("ObjectMethod", prx.ObjectMethod());
            Assert.AreEqual("ObjectMethod987", prx.ObjectMethod(987));
            Assert.AreEqual("ObjectMethod987str123", prx.ObjectMethod(987, "str123"));
            Assert.AreEqual("ObjectMethod987str123TestClass", prx.ObjectMethod(987, "str123", new TestClass()));
            Assert.AreEqual("ObjectMethod987str123TestClass34arg5arg6",
                prx.ObjectMethod(987, "str123", new TestClass(), 3, 4, "arg5", "arg6"));
        }

        /// <summary>
        /// Tests methods that exist in proxy interface, but do not exist in the actual service.
        /// </summary>
        [Test]
        public void TestMissingMethods()
        {
            var prx = GetProxy();

            var ex = Assert.Throws<InvalidOperationException>(() => prx.MissingMethod());

            Assert.AreEqual("Failed to invoke proxy: there is no method 'MissingMethod'" +
                            " in type 'Apache.Ignite.Core.Tests.Services.ServiceProxyTest+TestIgniteService'", ex.Message);
        }

        /// <summary>
        /// Tests ambiguous methods handling (multiple methods with the same signature).
        /// </summary>
        [Test]
        public void TestAmbiguousMethods()
        {
            var prx = GetProxy();

            var ex = Assert.Throws<InvalidOperationException>(() => prx.AmbiguousMethod(1));

            Assert.AreEqual("Failed to invoke proxy: there are 2 methods 'AmbiguousMethod' in type " +
                            "'Apache.Ignite.Core.Tests.Services.ServiceProxyTest+TestIgniteService' with (Int32) arguments, " +
                            "can't resolve ambiguity.", ex.Message);
        }

        [Test]
        public void TestException()
        {
            var prx = GetProxy();

            var err = Assert.Throws<ServiceInvocationException>(prx.ExceptionMethod);
            Assert.AreEqual("Expected exception", err.InnerException.Message);

            var ex = Assert.Throws<ServiceInvocationException>(() => prx.CustomExceptionMethod());
            Assert.IsTrue(ex.ToString().Contains("+CustomException"));
        }

        [Test]
        public void TestBinarizableMarshallingException()
        {
            var prx = GetProxy();
                
            var ex = Assert.Throws<ServiceInvocationException>(() => prx.CustomExceptionBinarizableMethod(false, false));

            if (KeepBinary)
            {
                Assert.AreEqual("Proxy method invocation failed with a binary error. " +
                                "Examine BinaryCause for details.", ex.Message);

                Assert.IsNotNull(ex.BinaryCause);
                Assert.IsNull(ex.InnerException);
            }
            else
            {
                Assert.AreEqual("Proxy method invocation failed with an exception. " +
                                "Examine InnerException for details.", ex.Message);

                Assert.IsNull(ex.BinaryCause);
                Assert.IsNotNull(ex.InnerException);
            }

            ex = Assert.Throws<ServiceInvocationException>(() => prx.CustomExceptionBinarizableMethod(true, false));
            Assert.IsTrue(ex.ToString().Contains(
                "Call completed with error, but error serialization failed [errType=CustomExceptionBinarizable, " +
                "serializationErrMsg=Expected exception in CustomExceptionBinarizable.WriteBinary]"));

            ex = Assert.Throws<ServiceInvocationException>(() => prx.CustomExceptionBinarizableMethod(true, true));
            Assert.IsTrue(ex.ToString().Contains(
                "Call completed with error, but error serialization failed [errType=CustomExceptionBinarizable, " +
                "serializationErrMsg=Expected exception in CustomExceptionBinarizable.WriteBinary]"));
        }

        /// <summary>
        /// Creates the proxy.
        /// </summary>
        protected ITestIgniteServiceProxyInterface GetProxy()
        {
            return GetProxy<ITestIgniteServiceProxyInterface>();
        }

        /// <summary>
        /// Creates the proxy.
        /// </summary>
        protected T GetProxy<T>()
        {
            _svc = new TestIgniteService(Binary);

            var prx = new ServiceProxy<T>(InvokeProxyMethod).GetTransparentProxy();

            Assert.IsFalse(ReferenceEquals(_svc, prx));

            return prx;
        }

        /// <summary>
        /// Invokes the proxy.
        /// </summary>
        /// <param name="method">Method.</param>
        /// <param name="args">Arguments.</param>
        /// <returns>
        /// Invocation result.
        /// </returns>
        private object InvokeProxyMethod(MethodBase method, object[] args)
        {
            using (var inStream = new PlatformMemoryStream(_memory.Allocate()))
            using (var outStream = new PlatformMemoryStream(_memory.Allocate()))
            {
                // 1) Write to a stream
                inStream.WriteBool(SrvKeepBinary);  // WriteProxyMethod does not do this, but Java does

                ServiceProxySerializer.WriteProxyMethod(_marsh.StartMarshal(inStream), method, args);

                inStream.SynchronizeOutput();

                inStream.Seek(0, SeekOrigin.Begin);

                // 2) call InvokeServiceMethod
                string mthdName;
                object[] mthdArgs;

                ServiceProxySerializer.ReadProxyMethod(inStream, _marsh, out mthdName, out mthdArgs);

                var result = ServiceProxyInvoker.InvokeServiceMethod(_svc, mthdName, mthdArgs);

                ServiceProxySerializer.WriteInvocationResult(outStream, _marsh, result.Key, result.Value);
                
                _marsh.StartMarshal(outStream).WriteString("unused");  // fake Java exception details

                outStream.SynchronizeOutput();

                outStream.Seek(0, SeekOrigin.Begin);

                return ServiceProxySerializer.ReadInvocationResult(outStream, _marsh, KeepBinary);
            }
        }

        /// <summary>
        /// Test service interface.
        /// </summary>
        protected interface ITestIgniteServiceProperties
        {
            /** */
            int IntProp { get; set; }

            /** */
            object ObjProp { get; set; }

            /** */
            string InvokeResult { get; }
        }

        /// <summary>
        /// Test service interface to check ambiguity handling.
        /// </summary>
        protected interface ITestIgniteServiceAmbiguity
        {
            /** */
            int AmbiguousMethod(int arg);
        }

        /// <summary>
        /// Test service interface.
        /// </summary>
        protected interface ITestIgniteService : ITestIgniteServiceProperties
        {
            /** */
            void VoidMethod();

            /** */
            void VoidMethod(int arg);

            /** */
            void VoidMethod(int arg, string arg1, object arg2 = null);

            /** */
            void VoidMethod(int arg, string arg1, object arg2 = null, params object[] args);

            /** */
            object ObjectMethod();

            /** */
            object ObjectMethod(int arg);

            /** */
            object ObjectMethod(int arg, string arg1, object arg2 = null);

            /** */
            object ObjectMethod(int arg, string arg1, object arg2 = null, params object[] args);

            /** */
            void ExceptionMethod();

            /** */
            void CustomExceptionMethod();

            /** */
            void CustomExceptionBinarizableMethod(bool throwOnWrite, bool throwOnRead);

            /** */
            TestBinarizableClass BinarizableArgMethod(int arg1, IBinaryObject arg2);

            /** */
            IBinaryObject BinarizableResultMethod(int arg1, TestBinarizableClass arg2);

            /** */
            IBinaryObject BinarizableArgAndResultMethod(int arg1, IBinaryObject arg2);

            /** */
            int AmbiguousMethod(int arg);
        }

        /// <summary>
        /// Test service interface. Does not derive from actual interface, but has all the same method signatures.
        /// </summary>
        protected interface ITestIgniteServiceProxyInterface
        {
            /** */
            int IntProp { get; set; }

            /** */
            object ObjProp { get; set; }

            /** */
            string InvokeResult { get; }

            /** */
            void VoidMethod();

            /** */
            void VoidMethod(int arg);

            /** */
            void VoidMethod(int arg, string arg1, object arg2 = null);

            /** */
            void VoidMethod(int arg, string arg1, object arg2 = null, params object[] args);

            /** */
            object ObjectMethod();

            /** */
            object ObjectMethod(int arg);

            /** */
            object ObjectMethod(int arg, string arg1, object arg2 = null);

            /** */
            object ObjectMethod(int arg, string arg1, object arg2 = null, params object[] args);

            /** */
            void ExceptionMethod();

            /** */
            void CustomExceptionMethod();

            /** */
            void CustomExceptionBinarizableMethod(bool throwOnWrite, bool throwOnRead);

            /** */
            TestBinarizableClass BinarizableArgMethod(int arg1, IBinaryObject arg2);

            /** */
            IBinaryObject BinarizableResultMethod(int arg1, TestBinarizableClass arg2);

            /** */
            IBinaryObject BinarizableArgAndResultMethod(int arg1, IBinaryObject arg2);

            /** */
            void MissingMethod();

            /** */
            int AmbiguousMethod(int arg);
        }

        /// <summary>
        /// Test service.
        /// </summary>
        [Serializable]
        private class TestIgniteService : ITestIgniteService, ITestIgniteServiceAmbiguity
        {
            /** */
            private readonly IBinary _binary;

            /// <summary>
            /// Initializes a new instance of the <see cref="TestIgniteService"/> class.
            /// </summary>
            /// <param name="binary">Binary.</param>
            public TestIgniteService(IBinary binary)
            {
                _binary = binary;
            }

            /** <inheritdoc /> */
            public int IntProp { get; set; }

            /** <inheritdoc /> */
            public object ObjProp { get; set; }

            /** <inheritdoc /> */
            public string InvokeResult { get; private set; }

            /** <inheritdoc /> */
            public void VoidMethod()
            {
                InvokeResult = "VoidMethod";
            }

            /** <inheritdoc /> */
            public void VoidMethod(int arg)
            {
                InvokeResult = "VoidMethod" + arg;
            }

            /** <inheritdoc /> */
            public void VoidMethod(int arg, string arg1, object arg2 = null)
            {
                InvokeResult = "VoidMethod" + arg + arg1 + arg2;
            }

            /** <inheritdoc /> */
            public void VoidMethod(int arg, string arg1, object arg2 = null, params object[] args)
            {
                InvokeResult = "VoidMethod" + arg + arg1 + arg2 + string.Concat(args.Select(x => x.ToString()));
            }

            /** <inheritdoc /> */
            public object ObjectMethod()
            {
                return "ObjectMethod";
            }

            /** <inheritdoc /> */
            public object ObjectMethod(int arg)
            {
                return "ObjectMethod" + arg;
            }

            /** <inheritdoc /> */
            public object ObjectMethod(int arg, string arg1, object arg2 = null)
            {
                return "ObjectMethod" + arg + arg1 + arg2;
            }

            /** <inheritdoc /> */
            public object ObjectMethod(int arg, string arg1, object arg2 = null, params object[] args)
            {
                return "ObjectMethod" + arg + arg1 + arg2 + string.Concat(args.Select(x => x.ToString()));
            }

            /** <inheritdoc /> */
            public void ExceptionMethod()
            {
                throw new ArithmeticException("Expected exception");
            }

            /** <inheritdoc /> */
            public void CustomExceptionMethod()
            {
                throw new CustomException();
            }

            /** <inheritdoc /> */
            public void CustomExceptionBinarizableMethod(bool throwOnWrite, bool throwOnRead)
            {
                throw new CustomExceptionBinarizable {ThrowOnRead = throwOnRead, ThrowOnWrite = throwOnWrite};
            }

            /** <inheritdoc /> */
            public TestBinarizableClass BinarizableArgMethod(int arg1, IBinaryObject arg2)
            {
                return arg2.Deserialize<TestBinarizableClass>();
            }

            /** <inheritdoc /> */
            public IBinaryObject BinarizableResultMethod(int arg1, TestBinarizableClass arg2)
            {
                return _binary.ToBinary<IBinaryObject>(arg2);
            }

            /** <inheritdoc /> */
            public IBinaryObject BinarizableArgAndResultMethod(int arg1, IBinaryObject arg2)
            {
                return _binary.ToBinary<IBinaryObject>(arg2.Deserialize<TestBinarizableClass>());
            }

            /** <inheritdoc /> */
            public override string ToString()
            {
                return IntProp.ToString();
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return IntProp.GetHashCode();
            }

            /** <inheritdoc /> */
            int ITestIgniteService.AmbiguousMethod(int arg)
            {
                return arg;
            }

            /** <inheritdoc /> */
            int ITestIgniteServiceAmbiguity.AmbiguousMethod(int arg)
            {
                return -arg;
            }
        }

        /// <summary>
        /// Test serializable class.
        /// </summary>
        [Serializable]
        private class TestClass
        {
            /** */
            public string Prop { get; set; }

            /** <inheritdoc /> */
            public override string ToString()
            {
                return "TestClass" + Prop;
            }
        }

        /// <summary>
        /// Custom non-serializable exception.
        /// </summary>
        private class CustomException : Exception
        {
            
        }

        /// <summary>
        /// Custom non-serializable exception.
        /// </summary>
        private class CustomExceptionBinarizable : Exception, IBinarizable
        {
            /** */
            public bool ThrowOnWrite { get; set; }

            /** */
            public bool ThrowOnRead { get; set; }

            /** <inheritdoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteBoolean("ThrowOnRead", ThrowOnRead);

                if (ThrowOnWrite)
                    throw new Exception("Expected exception in CustomExceptionBinarizable.WriteBinary");
            }

            /** <inheritdoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                ThrowOnRead = reader.ReadBoolean("ThrowOnRead");

                if (ThrowOnRead)
                    throw new Exception("Expected exception in CustomExceptionBinarizable.ReadBinary");
            }
        }

        /// <summary>
        /// Binarizable object for method argument/result.
        /// </summary>
        protected class TestBinarizableClass : IBinarizable
        {
            /** */
            public string Prop { get; set; }

            /** */
            public bool ThrowOnWrite { get; set; }

            /** */
            public bool ThrowOnRead { get; set; }

            /** <inheritdoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteString("Prop", Prop);
                writer.WriteBoolean("ThrowOnRead", ThrowOnRead);

                if (ThrowOnWrite)
                    throw new Exception("Expected exception in TestBinarizableClass.WriteBinary");
            }

            /** <inheritdoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                Prop = reader.ReadString("Prop");
                ThrowOnRead = reader.ReadBoolean("ThrowOnRead");

                if (ThrowOnRead)
                    throw new Exception("Expected exception in TestBinarizableClass.ReadBinary");
            }
        }
    }

    /// <summary>
    /// Tests <see cref="ServiceProxySerializer"/> functionality with keepBinary mode enabled on client.
    /// </summary>
    public class ServiceProxyTestKeepBinaryClient : ServiceProxyTest
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceProxyTestKeepBinaryClient"/> class.
        /// </summary>
        public ServiceProxyTestKeepBinaryClient()
        {
            KeepBinary = true;
        }

        [Test]
        public void TestBinarizableMethods()
        {
            var prx = GetProxy();

            var obj = new TestBinarizableClass { Prop = "PropValue" };

            var result = prx.BinarizableResultMethod(1, obj);

            Assert.AreEqual(obj.Prop, result.Deserialize<TestBinarizableClass>().Prop);
        }
    }

    /// <summary>
    /// Tests <see cref="ServiceProxySerializer"/> functionality with keepBinary mode enabled on server.
    /// </summary>
    public class ServiceProxyTestKeepBinaryServer : ServiceProxyTest
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceProxyTestKeepBinaryServer"/> class.
        /// </summary>
        public ServiceProxyTestKeepBinaryServer()
        {
            SrvKeepBinary = true;
        }

        [Test]
        public void TestBinarizableMethods()
        {
            var prx = GetProxy();

            var obj = new TestBinarizableClass { Prop = "PropValue" };
            var portObj = Binary.ToBinary<IBinaryObject>(obj);

            var result = prx.BinarizableArgMethod(1, portObj);

            Assert.AreEqual(obj.Prop, result.Prop);
        }
    }

    /// <summary>
    /// Tests <see cref="ServiceProxySerializer"/> functionality with keepBinary mode enabled on client and on server.
    /// </summary>
    public class ServiceProxyTestKeepBinaryClientServer : ServiceProxyTest
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceProxyTestKeepBinaryClientServer"/> class.
        /// </summary>
        public ServiceProxyTestKeepBinaryClientServer()
        {
            KeepBinary = true;
            SrvKeepBinary = true;
        }

        [Test]
        public void TestBinarizableMethods()
        {
            var prx = GetProxy();
            
            var obj = new TestBinarizableClass { Prop = "PropValue" };
            var portObj = Binary.ToBinary<IBinaryObject>(obj);

            var result = prx.BinarizableArgAndResultMethod(1, portObj);

            Assert.AreEqual(obj.Prop, result.Deserialize<TestBinarizableClass>().Prop);
        }
    }
}