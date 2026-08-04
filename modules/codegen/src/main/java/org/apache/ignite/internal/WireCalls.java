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
import org.apache.ignite.internal.MessageWireGenerator.Direction;

/**
 * Prints the {@code MessageWires} calls of a generated wire: taking a nested message field to the wire and back,
 * and preparing a cache object field. Which field to visit, in what loop and under what guard is
 * {@link MessageWireGenerator}'s business; this class only supplies the call at a leaf of the walk.
 */
public class WireCalls {
    /** Facade the generated code calls to take nested messages to the wire and back. */
    private static final String MESSAGE_WIRES_CLS = "org.apache.ignite.internal.managers.communication.MessageWires";

    /**
     * Generator whose class these calls go into; supplies the imports, indent and loop depth. A loop-nested call
     * reports itself back through {@code gen.usesMsgFactory}.
     */
    private final MessageWireGenerator gen;

    /** */
    WireCalls(MessageWireGenerator gen) {
        this.gen = gen;
    }

    /**
     * Generates a null-guarded {@code MessageWires} call. Loop-nested calls go through the overloads taking the
     * pre-resolved {@code msgFactory} local (see {@code prependMsgFactoryResolution}), so the factory is not
     * re-resolved from the context on every element.
     */
    List<String> forMessage(String accessor, Direction dir) {
        gen.imports.add(MESSAGE_WIRES_CLS);

        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("if (%s != null)", accessor));

        gen.indent++;

        if (gen.loopDepth > 0) {
            gen.usesMsgFactory = true;

            code.add(dir == Direction.OUT
                ? gen.indentedLine("MessageWires.prepare(msgFactory, %s, kctx, ctx);", accessor)
                : gen.indentedLine("MessageWires.restore(msgFactory, %s, kctx, ctx, clsLdr);", accessor));
        }
        else {
            code.add(dir == Direction.OUT
                ? gen.indentedLine("MessageWires.prepare(%s, kctx, ctx);", accessor)
                : gen.indentedLine("MessageWires.restore(%s, kctx, ctx, clsLdr);", accessor));
        }

        gen.indent--;

        return code;
    }

    /**
     * Generates a null-and-ctx-guarded call that prepares a {@code CacheObject} field, or reads it back. The one leaf
     * that does not go through {@code MessageWires}: a cache object is not a {@code Message} and has no wire of     * its own — it marshals itself against the cache object context.
     */
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

    /** Cache-free read-back of a {@code @NioField} message field on the NIO thread (no cache context available). */
    List<String> forNioMessage(String accessor) {
        gen.imports.add(MESSAGE_WIRES_CLS);

        List<String> code = new ArrayList<>();

        code.add(gen.indentedLine("if (%s != null)", accessor));

        gen.indent++;

        code.add(gen.indentedLine("MessageWires.restore(%s, kctx);", accessor));

        gen.indent--;

        return code;
    }
}
