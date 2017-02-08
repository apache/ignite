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

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.NotNull;

/**
 * ZeroMQ settings holder.
 */
public class ZeroMqSettings {
    /** */
    private int ioThreads;

    /** */
    private int type;

    /** */
    private String addr;

    /**
     * @param ioThreads Threads on context.
     * @param type Socket type.
     * @param addr Address to connect zmq.
     */
    public ZeroMqSettings(int ioThreads, int type, @NotNull String addr) {
        this.ioThreads = ioThreads;
        this.addr = addr;

        if (ZeroMqTypeSocket.check(type))
            this.type = type;
        else
            throw new IgniteException("This socket type not implementation this version ZeroMQ streamer.");
    }

    /**
     * @return Threads on context.
     */
    public int getIoThreads() {
        return ioThreads;
    }

    /**
     * @return Socket type.
     */
    public int getType() {
        return type;
    }

    /**
     * @return Address to connect zmq.
     */
    @NotNull public String getAddr() {
        return addr;
    }
}
