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

namespace Apache.Ignite.Examples.Thick.Misc.Services
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Services;

    /// <summary>
    /// This example demonstrates Ignite service deployment and execution.
    /// </summary>
    public static class Program
    {
        private const string ServiceName = "my-service";

        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine(">>> Services example started.");
                Console.WriteLine();

                // Deploy a service
                var svc = new MapService<int, string>();
                Console.WriteLine(">>> Deploying service to all nodes...");
                ignite.GetServices().DeployNodeSingleton(ServiceName, svc);

                // Get a sticky service proxy so that we will always be contacting the same remote node.
                var prx = ignite.GetServices().GetServiceProxy<IMapService<int, string>>(ServiceName, true);

                for (var i = 0; i < 10; i++)
                    prx.Put(i, i.ToString());

                var mapSize = prx.Size;

                Console.WriteLine(">>> Map service size: " + mapSize);

                ignite.GetServices().CancelAll();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Interface for service proxy interaction.
        /// Actual service class (<see cref="MapService{TK,TV}"/>) does not have to implement this interface.
        /// Target method/property will be searched by signature (name, arguments).
        /// </summary>
        public interface IMapService<TK, TV>
        {
            void Put(TK key, TV value);

            TV Get(TK key);

            void Clear();

            int Size { get; }
        }
    }
}
