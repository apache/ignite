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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.Map;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * InvokeAll response.
 */
class ClientCacheInvokeAllResponse extends ClientResponse {
    /** Result. */
    private final Map<Object, EntryProcessorResult<Object>> res;

    /**
     * Ctor.
     *
     * @param reqId Request id.
     * @param res Result.
     */
    ClientCacheInvokeAllResponse(long reqId, Map<Object, EntryProcessorResult<Object>> res) {
        super(reqId);

        assert res != null;

        this.res = res;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeInt(res.size());

        for (Map.Entry<Object, EntryProcessorResult<Object>> entry : res.entrySet()) {
            writer.writeObjectDetached(entry.getKey());
            EntryProcessorResult<Object> epRes = entry.getValue();

            try {
                Object val = epRes.get();
                writer.writeBoolean(true);
                writer.writeObjectDetached(val);
            }
            catch (EntryProcessorException e) {
                writer.writeBoolean(false);
                String msg = e.getMessage();

                if (ctx.kernalContext().clientListener().sendServerExceptionStackTraceToClient())
                    msg += U.nl() + X.getFullStackTrace(e);

                writer.writeString(msg);
            }
        }
    }
}
