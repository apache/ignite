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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * User message wrapper.
 */
@UseBinaryMarshaller
public class GridIoUserMessage implements Message {
    /** Message body. */
    private Object body;

    /** Serialized message body. */
    @Order(0)
    byte[] bodyBytes;

    /** Message topic. */
    private Object topic;

    /** Serialized message topic. */
    @Order(1)
    byte[] topicBytes;

    /** Deployment of the message classes. */
    @Order(2)
    GridDeploymentInfoMessage depInfo;

    /** Deployment class name. */
    @Order(3)
    String depClsName;

    /** Message deployment. */
    private GridDeployment dep;

    /**
     * @param body Message body.
     * @param bodyBytes Serialized message body.
     * @param depClsName Message body class name.
     * @param topic Message topic.
     * @param topicBytes Serialized message topic bytes.
     * @param depInfo Deployment of the message classes.
     */
    GridIoUserMessage(
        Object body,
        @Nullable byte[] bodyBytes,
        @Nullable String depClsName,
        @Nullable Object topic,
        @Nullable byte[] topicBytes,
        @Nullable GridDeploymentInfo depInfo) {
        this.body = body;
        this.bodyBytes = bodyBytes;
        this.depClsName = depClsName;
        this.topic = topic;
        this.topicBytes = topicBytes;
        this.depInfo = depInfo != null ? new GridDeploymentInfoMessage(depInfo) : null;
    }

    /**
     * Default constructor.
     */
    public GridIoUserMessage() {
        // No-op.
    }

    /**
     * @return Serialized message body.
     */
    @Nullable public byte[] bodyBytes() {
        return bodyBytes;
    }

    /**
     * @return Message body class name.
     */
    @Nullable public String deploymentClassName() {
        return depClsName;
    }

    /**
     * @return Serialized message topic.
     */
    @Nullable public byte[] topicBytes() {
        return topicBytes;
    }

    /**
     * @param topic New message topic.
     */
    public void topic(Object topic) {
        this.topic = topic;
    }

    /**
     * @return Message topic.
     */
    @Nullable public Object topic() {
        return topic;
    }

    /**
     * @param body New message body.
     */
    public void body(Object body) {
        this.body = body;
    }

    /** @return Deployment of the message classes, or {@code null} when peer class loading is off. */
    @Nullable public GridDeploymentInfo deploymentInfo() {
        return depInfo;
    }

    /**
     * @return Message body.
     */
    @Nullable public Object body() {
        return body;
    }

    /**
     * @param dep New message deployment.
     */
    public void deployment(GridDeployment dep) {
        this.dep = dep;
    }

    /**
     * @return Message deployment.
     */
    @Nullable public GridDeployment deployment() {
        return dep;
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIoUserMessage.class, this);
    }
}
