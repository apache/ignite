// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain {
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Diagnostics;
    using GridGain.Client;
    using GridGain.Client.Hasher;
    using GridGain.Client.Portable;

    using Dbg = System.Diagnostics.Debug;

    /** <summary>Start test suite main class.</summary> */
    public static class Program {
        [STAThread]
        static void Main(/*string[] args*/) {
            Debug.Listeners.Add(new TextWriterTraceListener(System.Console.Out));
            Debug.AutoFlush = true;

            //TestAll();

            //return;

            //Test(new GridClientPortableSelfTest(), (test) => test.TestClient());

            Test(new GridClientPortableSelfTest(), (test) => test.TestGenericCollections());
            Test(new GridClientPortableSelfTest(), (test) => test.TestCollectionsReflective());

            // 4. Handling simple fields inside object.
            Test(new GridClientPortableSelfTest(), (test) => test.TestPrimitiveFieldsReflective());
            Test(new GridClientPortableSelfTest(), (test) => test.TestPrimitiveFieldsPortable());
            Test(new GridClientPortableSelfTest(), (test) => test.TestPrimitiveFieldsRawPortable());
            Test(new GridClientPortableSelfTest(), (test) => test.TestPrimitiveFieldsSerializer());
            Test(new GridClientPortableSelfTest(), (test) => test.TestPrimitiveFieldsRawSerializer());

            // 1. Handling primitives.
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveBool());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveSbyte());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveByte());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveShort());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveUshort());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveChar());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveInt());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveUint());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveLong());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveUlong());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveFloat());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveDouble());

            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveBoolArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveSbyteArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveByteArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveShortArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveUshortArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveCharArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveIntArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveUintArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveLongArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveUlongArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveFloatArray());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWritePrimitiveDoubleArray());

            // 2. Handling strings.
            Test(new GridClientPortableSelfTest(), (test) => test.TestWriteString());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWriteStringArray());

            // 3. Handling Guids.
            Test(new GridClientPortableSelfTest(), (test) => test.TestWriteGuid());
            Test(new GridClientPortableSelfTest(), (test) => test.TestWriteGuidArray());

            Test(new GridClientPortableSelfTest(), (test) => test.TestObjectReflective());

            //TestAll();

            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestAffinity());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestAppendPrepend());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestCacheFlags());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestDataProjection());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestExecuteAsync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestExecuteSync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestGracefulShutdown());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestHaltShutdown());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestLogAsync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestLogSync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestMetricsAsync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestMetricsSync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestMultithreadedAccessToOneClient());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestNoAsyncExceptions());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestNodeMetricsAsync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestNodeMetricsSync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestOpenClose());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestPutAllAsync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestPutAllSync());
            //TestOne(new GridClientRouterTcpSslTest(), test => test.TestPutAsync());
            //TestOne(new GridClientTcpTest(), test => test.TestPutSync());
        }

        private static void Test<T>(T test, Action<T> job) where T : GridClientAbstractTest
        {
            test.InitClient();

            try
            {
                job(test);
            }
            finally
            {
                test.StopClient();
            }
        }

        private static void TestOne(GridClientAbstractTest test, Action<GridClientAbstractTest> job) {
            test.InitClient();

            try {
                job(test);
            }
            finally {
                test.StopClient();
            }
        }

        private static void TestAll() {
            string[] my_args = { Assembly.GetAssembly(typeof(GridClientTcpTest)).Location };

            int returnCode = NUnit.ConsoleRunner.Runner.Main(my_args);

            if (returnCode != 0)
                Console.Beep();
        }
    }
}
