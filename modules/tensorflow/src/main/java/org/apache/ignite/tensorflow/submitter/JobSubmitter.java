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
        Runnable cmd = parser.parse(args);

        if (cmd != null)
            cmd.run();
        else
            System.out.print(formatInfoMessage());
    }

    /**
     * Formats and returns info message.
     *
     * @return Info message.
     */
    private static String formatInfoMessage() {
        StringBuilder builder = new StringBuilder();

        builder.append("Usage: ignite-tf.sh COMMAND\n\n");
        builder.append("TensorFlow on Apache Ignite\n\n");
        builder.append("Options: \n");
        builder.append(" -c string    Location of Ignite node configuration\n\n");
        builder.append("Commands: \n");
        builder.append(" start  CACHE_NAME FOLDER COMMAND ARGS    Starts a new TensorFlow cluster\n");
        builder.append(" stop   CLUSTER_ID                        Stops a TensorFlow cluster\n");
        builder.append(" attach CLUSTER_ID                        Attaches to a running TensorFlow cluster\n");
        builder.append(" ps                                       Prints ids of all running TensorFlow clusters\n");

        return builder.toString();
    }
}
