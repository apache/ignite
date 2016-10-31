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

package org.apache.ignite.events;

import org.apache.ignite.cluster.ClusterNode;

/**
 * Cache fail event.
 */
public class UnhandledExceptionEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Exception ex;

    /**
     * Default constructor.
     */
    public UnhandledExceptionEvent() {
    }

    /**
     * @param node Node.
     * @param msg Message.
     * @param ex Exception.
     * @param type Type.
     */
    public UnhandledExceptionEvent(ClusterNode node, String msg, Exception ex, int type) {
        super(node, msg, type);
        this.ex = ex;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "msg=" + message() + ", type=" + type() + "ex=" + ex;
    }

    /**
     *
     * @return inner exception
     */
    public Exception getException() {
        return ex;
    }
}
