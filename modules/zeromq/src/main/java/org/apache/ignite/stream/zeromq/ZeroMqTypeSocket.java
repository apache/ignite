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
 * Socket types supported IgniteZeroMqStreamer.
 */
public enum ZeroMqTypeSocket {
    PAIR(ZMQ.PAIR),
    SUB(ZMQ.SUB),
    PULL(ZMQ.PULL);

    /**
     * Socket type.
     */
    private int type;

    ZeroMqTypeSocket(int type) {
        this.type = type;
    }

    /** */
    public int getType() {
        return type;
    }

    /**
     * Check socket type support.
     *
     * @param type
     * @return {@code True} if socket type is support.
     */
    public static boolean check(ZeroMqTypeSocket type) {
        for (ZeroMqTypeSocket ts : ZeroMqTypeSocket.values()) {
            if (ts.getType() == type.getType())
                return true;
        }
        return false;
    }
}
