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
using Apache.Ignite.Core.Lifecycle;
using Apache.Ignite.Core.Resource;

namespace Apache.Ignite.Examples.Misc
{
    /// <summary>
    /// This example shows how to provide your own <see cref="ILifecycleBean"/> implementation
    /// to be able to hook into GridGain lifecycle. Example bean will output occurred lifecycle 
    /// events to the console.
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project GridGainExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (GridGainExamples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// This example does not require remote nodes to be started.
    /// </summary>
    public class LifecycleExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            Console.WriteLine();
            Console.WriteLine(">>> Lifecycle example started.");

            // Create new configuration.
            var lifecycleExampleBean = new LifecycleExampleBean();

            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"modules\platform\src\main\dotnet\examples\config\example-compute.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" },
                LifecycleBeans = new List<ILifecycleBean> { lifecycleExampleBean }
            };

            // Provide lifecycle bean to configuration.
            using (Ignition.Start(cfg))
            {
                // Make sure that lifecycle bean was notified about grid startup.
                Console.WriteLine();
                Console.WriteLine(">>> Started (should be true): " + lifecycleExampleBean.Started);
            }

            // Make sure that lifecycle bean was notified about grid stop.
            Console.WriteLine();
            Console.WriteLine(">>> Started (should be false): " + lifecycleExampleBean.Started);

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Sample lifecycle bean implementation.
        /// </summary>
        private class LifecycleExampleBean : ILifecycleBean
        {
            /** Auto-inject grid instance. */
            [InstanceResource]
            private IIgnite _grid;

            /** <inheritDoc /> */
            public void OnLifecycleEvent(LifecycleEventType evt)
            {
                Console.WriteLine();
                Console.WriteLine(">>> Grid lifecycle event occurred: " + evt);
                Console.WriteLine(">>> Grid name: " + (_grid != null ? _grid.Name : "not available"));

                if (evt == LifecycleEventType.AfterNodeStart)
                    Started = true;
                else if (evt == LifecycleEventType.AfterNodeStop)
                    Started = false;          
            }

            /// <summary>
            /// Started flag.
            /// </summary>
            public bool Started
            {
                get;
                private set;
            }
        }
    }
}
