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
using System.Threading;
using GridGain.Cache.Event;
using GridGain.Cache.Query.Continuous;

namespace GridGain.Examples.Datagrid
{
    /// <summary>
    /// This example demonstrates continuous query API.
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project GridGainExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (GridGainExamples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run in conjunction with standalone GridGain .Net node.
    /// To start standalone node please do the following:
    /// 1) Build the project GridGainExamplesDll if you havent't done it yet (select it -> right-click -> Build);
    /// 2) Locate created GridGainExamplesDll.dll file (GridGainExamplesDll project -> right-click ->
    ///     Properties -> Build -> Output path);
    /// 3) Go to .Net binaries folder [GRIDGAIN_HOME]\platforms\dotnet and run GridGain.exe as follows:
    /// GridGain.exe -gridGainHome=[path_to_GRIDGAIN_HOME] -springConfigUrl=examples\config\dotnet\example-cache.xml -assembly=[path_to_GridGainExamplesDll.dll]
    /// </summary>
    public class ContinuousQueryExample
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
                JvmOptions = new List<string> {"-Xms512m", "-Xmx1024m"}
            };

            using (IGrid grid = GridFactory.Start(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache continuous query example started.");

                var cache = grid.GetOrCreateCache<int, string>("cache_continuous_query");

                // Clean up caches on all nodes before run.
                cache.Clear();

                const int keyCnt = 20;

                for (int i = 0; i < keyCnt; i++)
                    cache.Put(i, i.ToString());

                var qry = new ContinuousQuery<int, string>(new Listener<string>(), new ContinuousQueryFilter(15));


                // Create new continuous query.
                using (cache.QueryContinuous(qry))
                {
                    // Add a few more keys and watch more query notifications.
                    for (var i = keyCnt; i < keyCnt + 5; i++)
                        cache.Put(i, i.ToString());

                    // Wait for a while while callback is notified about remaining puts.
                    Thread.Sleep(2000);
                }
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Callback for continuous query example.
        /// </summary>
        private class Listener<T> : ICacheEntryEventListener<int, T>
        {
            public void OnEvent(IEnumerable<ICacheEntryEvent<int, T>> events)
            {
                foreach (var e in events)
                    Console.WriteLine("Queried entry [key=" + e.Key + ", val=" + e.Value + ']');
            }
        }
    }
}
