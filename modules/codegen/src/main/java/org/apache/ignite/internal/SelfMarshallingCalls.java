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

import java.util.ArrayList;
import java.util.List;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.internal.MessageMarshallerGenerator.Direction;

import static org.apache.ignite.internal.MessageProcessor.SELF_MARSHALLING_MESSAGE_INTERFACE;

/**
 * Prints the calls that take a field to its wire shape with no {@code Marshaller} in play: the object does the work
 * itself. Two kinds of them — the step a {@code SelfMarshallingMessage} writes by hand, and a {@code CacheObject}
 * preparing itself against the cache object context. Where to put them is {@link MessageMarshallerGenerator}'s business.
 */
public class SelfMarshallingCalls {
    /** Generator whose class these calls go into; supplies the indent and the current type. */
    private final MessageMarshallerGenerator gen;

    /** */
    private final TypeMirror selfMarshallingMsgType;

    /** Whether the message writes a step of its own. Read once per message. */
    private boolean selfMarshalling;

    /** */
    SelfMarshallingCalls(MessageMarshallerGenerator gen) {
        this.gen = gen;

        selfMarshallingMsgType = gen.type(SELF_MARSHALLING_MESSAGE_INTERFACE);
    }

    /** Reads what the type being generated for does itself. Called once per message. */
    void readType() {
        selfMarshalling = selfMarshallingMsgType != null && gen.assignableFrom(gen.type.asType(), selfMarshallingMsgType);
    }

    /** @return the step the message writes by hand, empty when it writes none. */
    List<String> ownStep(Direction dir) {
        if (!selfMarshalling)
            return List.of();

        return List.of(gen.indentedLine(dir == Direction.OUT ? "msg.selfMarshal();" : "msg.selfUnmarshal();"));
    }

    /** Generates a null-and-ctx-guarded call that prepares a {@code CacheObject} field, or reads it back. */
    List<String> forCacheObject(String accessor, Direction dir) {
        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("if (%s != null && ctx != null)", accessor));

        gen.indent++;

        code.add(dir == Direction.OUT
            ? gen.indentedLine("%s.marshal(ctx);", accessor)
            : gen.indentedLine("%s.unmarshal(ctx, clsLdr);", accessor));

        gen.indent--;

        return code;
    }
}
