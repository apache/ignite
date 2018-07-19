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

package org.apache.ignite.tensorflow.submitter;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.tensorflow.submitter.command.Command;
import org.apache.ignite.tensorflow.submitter.parser.CommandParser;
import org.apache.ignite.tensorflow.submitter.parser.GeneralCommandParser;

/**
 * Main class of the job submitter application that allows to submit TensorFlow jobs to be run within Ignite cluster.
 */
public class JobSubmitter {
    /** Command parser. */
    private static final CommandParser parser = new GeneralCommandParser();

    /**
     * Main method.
     *
     * @param args Arguments.
     */
    public static void main(String... args) {
        Command cmd = parser.parse(args);

        if (cmd != null)
            try (Ignite ignite = connectToCluster()) {
                cmd.runWithinIgnite(ignite);
            }
        else
            System.out.println(formatInfoMessage());
    }

    /**
     * Connects to Ignite cluster and returns Ignite client instance.
     *
     * @return Ignite client instance.
     */
    private static Ignite connectToCluster() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);

        return Ignition.start(cfg);
    }

    /**
     * Formats and returns info message.
     *
     * @return Info message.
     */
    private static String formatInfoMessage() {
        StringBuilder builder = new StringBuilder();

        builder.append("Usage: its.sh start CACHE_NAME JOB_ARCHIVE COMMAND").append("\n");
        builder.append("       Submits the specified job into Apache Ignite cluster with TensorFlow support.").append("\n");
        builder.append("\n");
        builder.append("      its.sh stop CLUSTER_ID").append("\n");
        builder.append("      Stops cluster with the specified cluster identifier.").append("\n");
        builder.append("\n");
        builder.append("      its.sh describe CLUSTER_ID").append("\n");
        builder.append("      Prints the description of cluster with specified cluster identifier.").append("\n");
        builder.append("\n");
        builder.append("      its.sh ps").append("\n");
        builder.append("      Prints list of running clusters.").append("\n");

        return builder.toString();
    }
}
