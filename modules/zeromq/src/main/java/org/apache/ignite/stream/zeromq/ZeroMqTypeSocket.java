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

package org.apache.ignite.stream.zeromq;

import org.zeromq.ZMQ;

/**
 * Enumeration of all supported IgniteZeroMqStreamer socket types.
 * <p>
 * The following types are supported:
 * <ul>
 * <li>{@link #PAIR}</li>
 * <li>{@link #SUB}</li>
 * <li>{@link #PULL}</li>
 * </ul>
 *
 */
public enum ZeroMqTypeSocket {
    /** For PAIR-PAIR pattern. */
    PAIR(ZMQ.PAIR),

    /** For PUB-SUB pattern. */
    SUB(ZMQ.SUB),

    /** For PUSH-PULL pattern */
    PULL(ZMQ.PULL);

    /** Socket type. */
    private int type;

    ZeroMqTypeSocket(int type) {
        this.type = type;
    }

    /** @return ZeroMQ original type. */
    public int getType() {
        return type;
    }
}
