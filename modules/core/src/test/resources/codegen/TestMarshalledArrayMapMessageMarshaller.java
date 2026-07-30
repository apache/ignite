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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopicMessage;
import org.apache.ignite.internal.TestMarshalledArrayMapMessage;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactory;
import org.apache.ignite.internal.managers.communication.MessageMarshalling;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public final class TestMarshalledArrayMapMessageMarshaller implements MessageMarshaller<TestMarshalledArrayMapMessage> {
    /** */
    @Override public void marshal(TestMarshalledArrayMapMessage msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        IgniteMessageFactory msgFactory = (IgniteMessageFactory)kctx.messageFactory();

        CacheObjectContext ctx = cacheObjCtx;

        if (msg.theMap != null && msg.mapKeys == null) {
            msg.mapKeys = new GridTopicMessage[msg.theMap.size()];
            msg.mapVals = new List[msg.mapKeys.length];
            int i = 0;
            for (Map.Entry<?, ?> e : msg.theMap.entrySet()) {
                msg.mapKeys[i] = (GridTopicMessage)e.getKey();
                msg.mapVals[i] = (List)e.getValue();
                i++;
            }
        }

        if (msg.fixedMap != null && msg.fixedMapKeys == null) {
            msg.fixedMapKeys = new GridTopicMessage[msg.fixedMap.size()];
            msg.fixedMapVals = new List[msg.fixedMapKeys.length];
            int i = 0;
            for (Map.Entry<?, ?> e : msg.fixedMap.entrySet()) {
                msg.fixedMapKeys[i] = (GridTopicMessage)e.getKey();
                msg.fixedMapVals[i] = (List)e.getValue();
                i++;
            }
        }

        if (msg.mapKeys != null) {
            for (GridTopicMessage e : msg.mapKeys) {
                if (e != null)
                    MessageMarshalling.marshal(msgFactory, e, kctx, ctx);
            }
        }

        if (msg.mapVals != null) {
            for (List e : msg.mapVals) {
                if (e != null) {
                    for (GridTopicMessage e1 : (Collection<? extends GridTopicMessage>)e) {
                        if (e1 != null)
                            MessageMarshalling.marshal(msgFactory, e1, kctx, ctx);
                    }
                }
            }
        }

        if (msg.fixedMapKeys != null) {
            for (GridTopicMessage e : msg.fixedMapKeys) {
                if (e != null)
                    MessageMarshalling.marshal(msgFactory, e, kctx, ctx);
            }
        }

        if (msg.fixedMapVals != null) {
            for (List e : msg.fixedMapVals) {
                if (e != null) {
                    for (GridTopicMessage e1 : (Collection<? extends GridTopicMessage>)e) {
                        if (e1 != null)
                            MessageMarshalling.marshal(msgFactory, e1, kctx, ctx);
                    }
                }
            }
        }
    }

    /** */
    @Override public void unmarshal(TestMarshalledArrayMapMessage msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
        IgniteMessageFactory msgFactory = (IgniteMessageFactory)kctx.messageFactory();

        CacheObjectContext ctx = cacheObjCtx;

        if (msg.mapKeys != null) {
            msg.theMap = U.newHashMap(msg.mapKeys.length);

            for (int i = 0; i < msg.mapKeys.length; i++) {
                GridTopicMessage k = msg.mapKeys[i];
                List v = msg.mapVals[i];

                if (k != null)
                    MessageMarshalling.unmarshal(k, kctx, ctx, clsLdr);

                if (v != null) {
                    for (GridTopicMessage e : (Collection<? extends GridTopicMessage>)v) {
                        if (e != null)
                            MessageMarshalling.unmarshal(msgFactory, e, kctx, ctx, clsLdr);
                    }
                }

                msg.theMap.put(k, v);
            }

            msg.mapKeys = null;
            msg.mapVals = null;
        }

        if (msg.fixedMapKeys != null) {
            for (int i = 0; i < msg.fixedMapKeys.length; i++) {
                GridTopicMessage k = msg.fixedMapKeys[i];
                List v = msg.fixedMapVals[i];

                if (k != null)
                    MessageMarshalling.unmarshal(k, kctx, ctx, clsLdr);

                if (v != null) {
                    for (GridTopicMessage e : (Collection<? extends GridTopicMessage>)v) {
                        if (e != null)
                            MessageMarshalling.unmarshal(msgFactory, e, kctx, ctx, clsLdr);
                    }
                }

                msg.fixedMap.put(k, v);
            }

            msg.fixedMapKeys = null;
            msg.fixedMapVals = null;
        }
    }
}