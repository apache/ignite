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

package org.apache.ignite.marshaller;

import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Utility marshaller methods.
 */
public class MarshallerUtils {
    /** Job sender node version. */
    private static final ThreadLocal<IgniteProductVersion> JOB_SND_NODE_VER = new ThreadLocal<>();

    /**
     * Set node name to marshaller context if possible.
     *
     * @param marsh Marshaller instance.
     * @param nodeName Node name.
     */
    public static void setNodeName(Marshaller marsh, @Nullable String nodeName) {
        if (marsh instanceof AbstractNodeNameAwareMarshaller)
            ((AbstractNodeNameAwareMarshaller)marsh).nodeName(nodeName);
    }

    /**
     * Create JDK marshaller with provided node name.
     *
     * @param nodeName Node name.
     * @return JDK marshaller.
     */
    public static JdkMarshaller jdkMarshaller(@Nullable String nodeName) {
        JdkMarshaller marsh = new JdkMarshaller();

        setNodeName(new JdkMarshaller(), nodeName);

        return marsh;
    }

    /**
     * Private constructor.
     */
    private MarshallerUtils() {
        // No-op.
    }

    /**
     * Sets thread local job sender node version.
     *
     * @param ver Thread local job sender node version.
     */
    public static void jobSenderVersion(IgniteProductVersion ver) {
        JOB_SND_NODE_VER.set(ver);
    }

    /**
     * Returns thread local job sender node version.
     *
     * @return Thread local job sender node version.
     */
    public static IgniteProductVersion jobSenderVersion() {
        return JOB_SND_NODE_VER.get();
    }
}
