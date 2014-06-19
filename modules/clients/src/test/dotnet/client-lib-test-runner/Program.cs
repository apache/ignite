// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain {
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Diagnostics;
    using GridGain.Client;
    using GridGain.Client.Hasher;
    using GridGain.Client.Impl.Marshaller;

    using Dbg = System.Diagnostics.Debug;

    /** <summary>Start test suite main class.</summary> */
    public static class Program {
        [STAThread]
        static void Main(/*string[] args*/) {
            Debug.Listeners.Add(new TextWriterTraceListener(System.Console.Out));
            Debug.AutoFlush = true;

            Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.High;

            TestAll();

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
            string[] my_args = { Assembly.GetAssembly(typeof(GridClientHttpTest)).Location };

            int returnCode = NUnit.ConsoleRunner.Runner.Main(my_args);

            if (returnCode != 0)
                Console.Beep();
        }
    }
}
