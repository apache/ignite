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

package org.apache.ignite.loadtests.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;

import javax.swing.*;

/**
 * Node startup for GGFS performance benchmark.
 */
public class IgfsNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite ignored = G.start("config/hadoop/default-config.xml")) {
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
