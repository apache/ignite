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

namespace Apache.Ignite.Examples.Misc
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// This example shows how to provide your own <see cref="ILifecycleHandler"/> implementation
    /// to be able to hook into Apache lifecycle. Example bean will output occurred lifecycle 
    /// events to the console.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
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
            var lifecycleAwareExample = new LifecycleHandlerExample();

            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"127.0.0.1:47500"}
                    }
                },
                LifecycleHandlers = new[] {lifecycleAwareExample}
            };

            // Provide lifecycle bean to configuration.
            using (Ignition.Start(cfg))
            {
                // Make sure that lifecycle bean was notified about Ignite startup.
                Console.WriteLine();
                Console.WriteLine(">>> Started (should be true): " + lifecycleAwareExample.Started);
            }

            // Make sure that lifecycle bean was notified about Ignite stop.
            Console.WriteLine();
            Console.WriteLine(">>> Started (should be false): " + lifecycleAwareExample.Started);

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Sample lifecycle bean implementation.
        /// </summary>
        private class LifecycleHandlerExample : ILifecycleHandler
        {
            /** Auto-injected Ignite instance. */
            [InstanceResource]
#pragma warning disable 649
            private IIgnite _ignite;
#pragma warning restore 649

            /** <inheritDoc /> */
            public void OnLifecycleEvent(LifecycleEventType evt)
            {
                Console.WriteLine();
                Console.WriteLine(">>> Ignite lifecycle event occurred: " + evt);
                Console.WriteLine(">>> Ignite name: " + (_ignite != null ? _ignite.Name : "not available"));

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
