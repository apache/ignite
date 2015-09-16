/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using System.Collections.Generic;
using GridGain.Examples.Portable;
using GridGain.Transactions;

namespace GridGain.Examples.Datagrid
{
    /// <summary>
    /// This example demonstrates how to use transactions on GridGain cache.
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project GridGainExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (GridGainExamples project -> right-click -> Properties ->
    ///    Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run in conjunction with standalone GridGain .Net node.
    /// To start standalone node please do the following:
    /// 1) Go to .Net binaries folder [GRIDGAIN_HOME]\platforms\dotnet and run GridGain.exe as follows:
    /// GridGain.exe -gridGainHome=[path_to_GRIDGAIN_HOME] -springConfigUrl=examples\config\dotnet\example-cache.xml
    /// </summary>
    class TransactionExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            GridConfiguration cfg = new GridConfiguration
            {
                SpringConfigUrl = @"examples\config\dotnet\example-cache.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };

            using (IGrid grid = GridFactory.Start(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Transaction example started.");

                var cache = grid.Cache<int, Account>("tx");

                // Clean up caches on all nodes before run.
                cache.Clear();

                // Initialize.
                cache.Put(1, new Account(1, 100));
                cache.Put(2, new Account(2, 200));

                Console.WriteLine();
                Console.WriteLine(">>> Accounts before transfer: ");
                Console.WriteLine(">>>     " + cache.Get(1));
                Console.WriteLine(">>>     " + cache.Get(2));
                Console.WriteLine();

                // Transfer money between accounts in a single transaction.
                using (var tx = cache.Grid.Transactions.TxStart(TransactionConcurrency.PESSIMISTIC,
                    TransactionIsolation.REPEATABLE_READ))
                {
                    Account acc1 = cache.Get(1);
                    Account acc2 = cache.Get(2);

                    acc1.Balance += 100;
                    acc2.Balance -= 100;

                    cache.Put(1, acc1);
                    cache.Put(2, acc2);

                    tx.Commit();
                }

                Console.WriteLine(">>> Transfer finished.");

                Console.WriteLine();
                Console.WriteLine(">>> Accounts after transfer: ");
                Console.WriteLine(">>>     " + cache.Get(1));
                Console.WriteLine(">>>     " + cache.Get(2));
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
