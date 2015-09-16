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
using System.Linq;

namespace GridGain.Examples.Compute
{
    /// <summary>
    /// Example demonstrating closure execution.
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
    /// GridGain.exe -gridGainHome=[path_to_GRIDGAIN_HOME] -springConfigUrl=examples\config\dotnet\example-compute.xml -assembly=[path_to_GridGainExamplesDll.dll]
    /// <para />
    /// As a result you will see console jobs output on one or several nodes.
    /// </summary>
    public class ClosureExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            GridConfiguration cfg = new GridConfiguration
            {
                SpringConfigUrl = @"examples\config\dotnet\example-compute.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };
            
            using (IGrid grid = GridFactory.Start(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Closure execution example started.");

                // Split the string by spaces to count letters in each word in parallel.
                ICollection<string> words = "Count characters using closure".Split().ToList();

                Console.WriteLine();
                Console.WriteLine(">>> Calculating character count with manual reducing:");

                var res = grid.Compute().Apply(new CharacterCountClosure(), words);

                int totalLen = res.Sum();

                Console.WriteLine(">>> Total character count: " + totalLen);
                Console.WriteLine();
                Console.WriteLine(">>> Calculating character count with reducer:");

                totalLen = grid.Compute().Apply(new CharacterCountClosure(), words, new CharacterCountReducer());

                Console.WriteLine(">>> Total character count: " + totalLen);
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
