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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.jupiter.api.Test;

public class Discovery {

    @Test
    void clientsBehindNat() {

        //tag::client-behind-nat[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        
        cfg.setClientMode(true);

        cfg.setCommunicationSpi(new TcpCommunicationSpi().setForceClientToServerConnections(true));

        //end::client-behind-nat[]
        try(Ignite ignite = Ignition.start(cfg)) {
            
        } 
    }
}
