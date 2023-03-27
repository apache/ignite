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

using Apache.Ignite.Core;
using Apache.Ignite.Core.Communication.Tcp;

namespace dotnet_helloworld
{
    class ClusteringOverview
    {
        static void Foo()
        {
            // tag::ClientsAndServers[]
            Ignition.ClientMode = true;
            Ignition.Start();
            // end::ClientsAndServers[]

             // tag::CommunicationSPI[]
             var cfg = new IgniteConfiguration
             {
                 CommunicationSpi = new TcpCommunicationSpi
                 {
                     LocalPort = 1234
                 }
             };
            Ignition.Start(cfg);
            // end::CommunicationSPI[]
        }

        static void ClientCfg()
        {
            // tag::ClientCfg[]
            var cfg = new IgniteConfiguration
            {
                // Enable client mode.
                ClientMode = true
            };
            
            // Start Ignite in client mode.
            Ignition.Start(cfg);
            // end::ClientCfg[]
        }
    }
}
