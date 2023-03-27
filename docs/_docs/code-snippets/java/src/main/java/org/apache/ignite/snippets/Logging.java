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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.jcl.JclLogger;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.logger.slf4j.Slf4jLogger;

public class Logging {
    void log4j2() throws IgniteCheckedException {
        // tag::log4j2[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        IgniteLogger log = new Log4J2Logger("log4j2-config.xml");

        cfg.setGridLogger(log);

        // Start a node.
        try (Ignite ignite = Ignition.start(cfg)) {
            ignite.log().info("Info Message Logged!");
        }
        // end::log4j2[]
    }

    void jcl() {
        //tag::jcl[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(new JclLogger());

        // Start a node.
        try (Ignite ignite = Ignition.start(cfg)) {
            ignite.log().info("Info Message Logged!");
        }
        //end::jcl[]
    }
    
       void slf4j() {
        //tag::slf4j[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(new Slf4jLogger());

        // Start a node.
        try (Ignite ignite = Ignition.start(cfg)) {
            ignite.log().info("Info Message Logged!");
        }
        //end::slf4j[]
    }

    public static void main(String[] args) throws IgniteCheckedException {
        Logging logging = new Logging();

        logging.jcl();
        logging.slf4j();
    }
}
