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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication;

import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopProcessDescriptor;

/**
 * Hadoop communication message listener.
 */
public interface HadoopMessageListener {
    /**
     * @param desc Process descriptor.
     * @param msg Hadoop message.
     */
    public void onMessageReceived(HadoopProcessDescriptor desc, HadoopMessage msg);

    /**
     * Called when connection to remote process was lost.
     *
     * @param desc Process descriptor.
     */
    public void onConnectionLost(HadoopProcessDescriptor desc);
}