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

using System;
using System.Collections.Generic;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Transactions;
using Apache.Ignite.ExamplesDll.Portable;

namespace Apache.Ignite.Examples.Datagrid
{
    /// <summary>
    /// This example demonstrates how to use transactions on Apache cache.
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///    Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run in conjunction with standalone Apache Ignite .Net node.
    /// To start standalone node please do the following:
    /// 1) Locate Apache.Ignite.exe file (Apache.Ignite project -> right-click -> Properties -> Build -> Output path)
    /// Apache.Ignite.exe -IgniteHome=[path_to_IGNITE_HOME] -springConfigUrl=modules\platform\src\main\dotnet\examples\config\example-cache.xml
    /// </summary>
    class TransactionExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"modules\platform\src\main\dotnet\examples\config\example-cache.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Transaction example started.");

                var cache = ignite.Cache<int, Account>("tx");

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
                using (var tx = cache.Ignite.Transactions.TxStart(TransactionConcurrency.Pessimistic,
                    TransactionIsolation.RepeatableRead))
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
