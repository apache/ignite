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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;

import static org.apache.ignite.internal.MessageProcessor.CACHE_OBJECT_CLS;
import static org.apache.ignite.internal.MessageProcessor.MESSAGE_INTERFACE;
import static org.apache.ignite.internal.MessageProcessor.NON_MARSHALLABLE_MESSAGE_INTERFACE;

/**
 * Shared ground for the companions that take a {@code Message} to the wire and back: walking its fields, telling a
 * nested message from a cache object from a collection, and the loop and null-guard shapes the generated code uses.
 * What each companion does with a field is left to the subclass.
 */
public abstract class MessageWireCompanionGenerator extends MessageCompanionGenerator {
    /** {@inheritDoc} */
    @Override protected boolean shouldSkip(TypeElement type, List<VariableElement> fields) {
        return isNonMarshallable(type.asType());
    }

    /** Prefixes {@code body} with the {@code msgFactory} resolution line when a loop-nested facade call was emitted. */
    protected void prependMsgFactoryResolution(List<String> body) {
        if (!usesMsgFactory)
            return;

        imports.add(IGNITE_MESSAGE_FACTORY_CLS);

        body.add(0, EMPTY);
        body.add(0, indentedLine("IgniteMessageFactory msgFactory = (IgniteMessageFactory)kctx.messageFactory();"));
    }

    /**
     * Returns the {@code CacheObjectContext ctx} resolution line for the current message type. Cache messages resolve
     * via the cache, group messages via the cache group — the group's context outlives the stop of individual caches,
     * so cache objects still unmarshal while a cache (group) is being destroyed.
     */
    protected String ctxResolutionLine() {
        if (isCacheIdAwareMessage(type))
            return indentedLine("CacheObjectContext ctx = cacheObjCtx != null ? cacheObjCtx : " +
                    "kctx.cache().context().cacheObjectContext(msg.cacheId());");
        else if (isCacheGroupIdMessage(type))
            return indentedLine("CacheObjectContext ctx = cacheObjCtx != null ? cacheObjCtx : " +
                    "kctx.cache().cacheGroup(msg.groupId()) == null ? null : " +
                    "kctx.cache().cacheGroup(msg.groupId()).cacheObjectContext();");
        else
            return indentedLine("CacheObjectContext ctx = cacheObjCtx;");
    }

    /** Recursion skip for such fields is subtype-safe: subclasses inherit the {@code NonMarshallableMessage} marker. */
    protected boolean isNonMarshallable(TypeMirror t) {
        return assignableFrom(t, nonMarshallableType);
    }

    /** */
    protected boolean isCacheGroupIdMessage(TypeElement te) {
        return assignableFrom(te.asType(), cacheGrpIdMsgType);
    }

    /** */
    protected static final String GRID_KERNAL_CONTEXT_CLS = "org.apache.ignite.internal.GridKernalContext";

    /** */
    protected static final String CACHE_OBJECT_CONTEXT_CLS = "org.apache.ignite.internal.processors.cache.CacheObjectContext";

    /** */
    protected static final String GRID_CACHE_GROUP_ID_MESSAGE_CLS = "org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage";

    /** Facade the generated code calls to take nested messages to the wire and back. */
    protected static final String MESSAGE_WIRE_CLS = "org.apache.ignite.internal.managers.communication.MessageWire";

    /** */
    protected static final String IGNITE_MESSAGE_FACTORY_CLS = "org.apache.ignite.internal.managers.communication.IgniteMessageFactory";

    /** Accumulated source lines of the generated methods. */
    protected final List<String> methods = new ArrayList<>();

    /** */
    protected final TypeMirror msgType;

    /** */
    protected final TypeMirror cacheObjType;

    /** */
    protected final TypeMirror nonMarshallableType;

    /** */
    protected final TypeMirror cacheGrpIdMsgType;

    /** */
    protected final TypeMirror mapType;

    /** */
    protected final TypeMirror colType;

    /** Whether any generated method got a non-empty body; a companion without one is skipped entirely. */
    protected boolean hasStatements;

    /** Nesting depth of the current for-loop; names loop variables {@code e}, {@code e1}, {@code e2}… */
    protected int loopDepth;

    /** Whether the currently generated method emitted a loop-nested facade call and so needs the {@code msgFactory} local. */
    protected boolean usesMsgFactory;

    /** */
    MessageWireCompanionGenerator(ProcessingEnvironment env) {
        super(env);

        msgType = type(MESSAGE_INTERFACE);
        cacheObjType = type(CACHE_OBJECT_CLS);
        nonMarshallableType = type(NON_MARSHALLABLE_MESSAGE_INTERFACE);
        cacheGrpIdMsgType = type(GRID_CACHE_GROUP_ID_MESSAGE_CLS);
        mapType = type(Map.class.getName());
        colType = type(Collection.class.getName());
    }

    /** */
    protected boolean isMessage(TypeMirror type) {
        return assignableFrom(type, msgType);
    }

    /** */
    protected boolean isCacheObject(TypeMirror type) {
        return assignableFrom(type, cacheObjType);
    }

    /** Returns {@code true} if {@code type} (erased) is assignable to {@code java.util.Map}. */
    protected boolean isMap(TypeMirror type) {
        return assignableFrom(erasedType(type), mapType);
    }

    /** Returns {@code true} if {@code type} (erased) is assignable to {@code java.util.Collection}. */
    protected boolean isCollection(TypeMirror type) {
        return assignableFrom(erasedType(type), colType);
    }

}
