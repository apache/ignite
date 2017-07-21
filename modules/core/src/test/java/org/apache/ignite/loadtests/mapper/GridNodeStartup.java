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

package org.apache.ignite.loadtests.mapper;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.G;

/**
 * Starts up an empty node with cache configuration.
 * You can also start a stand-alone Ignite instance by passing the path
 * to configuration file to {@code 'ignite.{sh|bat}'} script, like so:
 * {@code 'ignite.sh examples/config/example-cache.xml'}.
 * <p>
 * The difference is that running this class from IDE adds all example classes to classpath
 * but running from command line doesn't.
 */
public class GridNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignored = G.start("examples/config/example-cache.xml")) {
            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[] {
                    new JLabel("Ignite started."),
                    new JLabel("Press OK to stop Ignite.")
                },
                "Ignite",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
    }
}