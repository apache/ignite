/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.util.Collections;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;

/**
 *
 */
public class CommandHandler {
    /**
     * @param args Args.
     */
    public static void main(String[] args) throws GridClientException {
        String host = "127.0.0.1";
        String port = "11212";
        Boolean activate = null;

        if (args.length == 1 && "help".equals(args[0])){
            System.out.println("Example: --host {ip} --port {port} --{activate/deactivate} " +
                "or without command --host {ip} --port {port} then will print status.");

            return;
        }

        if (args.length > 5)
            throw new IllegalArgumentException("incorrect number of arguments");

        for (int i = 0; i < args.length; i++) {
            String str = args[i];

            if ("--host".equals(str))
                host = args[i + 1];
            else if ("--port".equals(str))
                port = args[i + 1];
            else if ("--activate".equals(str))
                activate = true;
            else if ("--deactivate".equals(str))
                activate = false;
        }

        if (host == null)
            throw new IllegalArgumentException("host can not be empty");

        if (port == null)
            throw new IllegalArgumentException("port can not be empty");

        GridClientConfiguration cfg = new GridClientConfiguration();
        cfg.setServers(Collections.singletonList(host + ":" + port));

        try (GridClient client = GridClientFactory.start(cfg)) {
            GridClientClusterState state = client.state();

            if (activate != null)
                try {
                    state.active(activate);

                    System.out.println(host + ":" + port + " - was " + (activate ? "activate" : "deactivate"));
                }
                catch (Exception e) {
                    System.out.println("Something fail during " + (activate ? "activation" : "deactivation")
                        + ", exception message: " + e.getMessage());
                }
            else
                System.out.println(host + ":" + port + " - " + (state.active() ? "active" : "inactive"));

        }
    }
}
