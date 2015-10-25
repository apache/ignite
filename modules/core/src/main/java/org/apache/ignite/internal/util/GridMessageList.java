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

package org.apache.ignite.internal.util;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * List of messages.
 */
public class GridMessageList<X extends Message> implements Message {
    /** */
    @GridDirectCollection(Message.class)
    private List<X> list;

    /**
     * @param arr Zero or more messages.
     * @return Message list.
     */
    public static <Z  extends Message> GridMessageList<Z> asList(Z...arr) {
        GridMessageList<Z> res = new GridMessageList<>();

        res.list(F.asList(arr));

        return res;
    }

    /**
     * @param list List of messages.
     */
    public void list(List<X> list) {
        this.list = list;
    }

    /**
     * @return List of messages.
     */
    public List<X> list() {
        return list;
    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return false;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }

    @Override public byte directType() {
        return 114;
    }

    @Override public byte fieldsCount() {
        return 0;
    }
}
