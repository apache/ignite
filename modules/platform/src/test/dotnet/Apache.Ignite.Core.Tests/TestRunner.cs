namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Diagnostics;
    using System.Reflection;

    public static class TestRunner
    {
        [STAThread]
        static void Main()
        {
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            Debug.AutoFlush = true;

            //TestOne(typeof(ContinuousQueryAtomiclBackupTest), "TestInitialQuery");

            TestAll(typeof(GridFactoryTest));

            //TestAllInAssembly();
        }

        private static void TestOne(Type testClass, string method)
        {
            string[] args = { "/run:" + testClass.FullName + "." + method, Assembly.GetAssembly(testClass).Location };

            int returnCode = NUnit.ConsoleRunner.Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();
        }

        private static void TestAll(Type testClass)
        {
            string[] args = { "/run:" + testClass.FullName, Assembly.GetAssembly(testClass).Location };

            int returnCode = NUnit.ConsoleRunner.Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();
        }

        private static void TestAllInAssembly()
        {
            string[] args = { Assembly.GetAssembly(typeof(GridFactoryTest)).Location };

            int returnCode = NUnit.ConsoleRunner.Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();
        }

    }
}
