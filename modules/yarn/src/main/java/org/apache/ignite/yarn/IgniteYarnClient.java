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

package org.apache.ignite.yarn;

import org.apache.hadoop.yarn.client.api.*;
import org.apache.hadoop.yarn.conf.*;

import java.util.logging.*;

/**
 * Ignite yarn client.
 */
public class IgniteYarnClient {
    /** */
    public static final Logger log = Logger.getLogger(IgniteYarnClient.class.getSimpleName());

    /**
     * Main methods has only one optional parameter - path to properties files.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        ClusterProperties clusterProps = ClusterProperties.from(args.length >= 1 ? args[0] : null);

        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();

        YarnClient yarnClient = YarnClient.createYarnClient();

        yarnClient.init(conf);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();
    }
}
