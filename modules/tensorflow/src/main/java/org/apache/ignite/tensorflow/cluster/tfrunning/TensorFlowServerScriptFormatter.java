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

package org.apache.ignite.tensorflow.cluster.tfrunning;

import org.apache.ignite.Ignite;

/**
 * Utils class that helps to format Python script that starts TensorFlow server.
 */
public class TensorFlowServerScriptFormatter {
    /**
     * Formats TensorFlow server specification so that it's available to be passed into а python script.
     *
     * @param srv Server specification.
     * @param join Joins server by default or not.
     * @param ignite Ignite instance.
     * @return Formatted TensorFlow server script.
     */
    public String format(TensorFlowServer srv, boolean join, Ignite ignite) {
        StringBuilder builder = new StringBuilder();

        builder.append("from __future__ import absolute_import").append("\n");
        builder.append("from __future__ import division").append("\n");
        builder.append("from __future__ import print_function").append("\n");

        builder.append("from threading import Thread").append("\n");
        builder.append("from time import sleep").append("\n");
        builder.append("import os, signal").append("\n");

        builder.append("\n");

        builder.append("def check_pid(pid):").append("\n");
        builder.append("    try:").append("\n");
        builder.append("        os.kill(pid, 0)").append("\n");
        builder.append("    except OSError:").append("\n");
        builder.append("        return False").append("\n");
        builder.append("    else:").append("\n");
        builder.append("        return True").append("\n");

        builder.append("\n");

        builder.append("def threaded_function(pid):").append("\n");
        builder.append("    while check_pid(pid):").append("\n");
        builder.append("        sleep(1)").append("\n");
        builder.append("    os.kill(os.getpid(), signal.SIGUSR1)").append("\n");

        builder.append("\n");

        builder.append("Thread(target = threaded_function, args = (int(os.environ['PPID']), )).start()")
            .append("\n");
        builder.append("\n");

        builder.append("import tensorflow as tf").append('\n');
        builder.append("fto_import_contrib_ops = tf.contrib.resampler").append("\n");
        builder.append("import tensorflow.contrib.igfs.python.ops.igfs_ops").append("\n");

        builder.append("print('job:%s task:%d' % ('")
            .append(srv.getJobName())
            .append("', ")
            .append(srv.getTaskIdx())
            .append("))")
            .append("\n");
        builder.append("print('IGNITE_DATASET_HOST = ', os.environ.get('IGNITE_DATASET_HOST'))").append("\n");
        builder.append("print('IGNITE_DATASET_PORT = ', os.environ.get('IGNITE_DATASET_PORT'))").append("\n");
        builder.append("print('IGNITE_DATASET_PART = ', os.environ.get('IGNITE_DATASET_PART'))").append("\n");

        builder.append("os.environ['TF_CONFIG'] = '").append(formatTfConfigVar(srv, ignite)).append("'\n");
        builder.append("server = tf.contrib.distribute.run_standard_tensorflow_server()").append("\n");

        if (join)
            builder.append("server.join()").append('\n');

        return builder.toString();
    }

    /**
     * Formats "TF_CONFIG" variable to be passed into user script.
     *
     * @param srv Server description.
     * @param ignite Ignite instance.
     * @return Formatted "TF_CONFIG" variable to be passed into user script.
     */
    private String formatTfConfigVar(TensorFlowServer srv, Ignite ignite) {
        return "{\"cluster\" : " +
            srv.getClusterSpec().format(ignite).replace('\n', ' ') +
            ", " +
            "\"task\": {\"type\" : \"" +
            srv.getJobName() +
            "\", \"index\": " +
            srv.getTaskIdx() +
            "}}";
    }
}
