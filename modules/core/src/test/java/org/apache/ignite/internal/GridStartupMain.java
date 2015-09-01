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

package org.apache.ignite.internal;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import org.apache.ignite.internal.util.typedef.G;

/**
 * Ignite startup.
 */
public class GridStartupMain {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        //resetLog4j("org.apache.ignite.internal.processors.cache.distributed.dht.preloader", Level.DEBUG, false, 0);

        //G.start("modules/tests/config/spring-multicache.xml");
        //G.start("examples/config/example-cache.xml");

        G.start();

        // Wait until Ok is pressed.
        JOptionPane.showMessageDialog(
            null,
            new JComponent[] {
                new JLabel("Ignite started."),
                new JLabel(
                    "<html>" +
                        "You can use JMX console at <u>http://localhost:1234</u>" +
                    "</html>"),
                new JLabel("Press OK to stop Ignite.")
            },
            "Ignite Startup JUnit",
            JOptionPane.INFORMATION_MESSAGE
        );

        G.stop(true);
    }
}