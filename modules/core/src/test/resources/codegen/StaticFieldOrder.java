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

package org.apache.ignite.internal;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class StaticFieldOrder implements Message {
    @Order(0)
    public static int id;

    public int id() {
        return id;
    }

    public void id(int id) {
        this.id = id;
    }

    public short directType() {
        return 0;
    }

    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return true;
    }

    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return true;
    }

    public void onAckReceived() {
        // No-op.
    }
}
