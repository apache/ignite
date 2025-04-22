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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientObjectResponse;
import org.apache.ignite.internal.processors.platform.client.ClientPlatform;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cache invoke request.
 */
public class ClientCacheInvokeRequest extends ClientCacheKeyRequest {
    /** */
    private final EntryProcessorReader entryProcReader;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCacheInvokeRequest(BinaryReaderEx reader) {
        super(reader);

        entryProcReader = new EntryProcessorReader(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process0(ClientConnectionContext ctx) {
        try {
            Object val = cache(ctx).invoke(key(), entryProcReader.getEntryProcessor(),
                entryProcReader.getArgs(isKeepBinary()));

            return new ClientObjectResponse(requestId(), val);
        }
        catch (EntryProcessorException e) {
            throw new IgniteClientException(ClientStatus.ENTRY_PROCESSOR_EXCEPTION, e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<ClientResponse> processAsync0(ClientConnectionContext ctx) {
        return chainFuture(
            cache(ctx).invokeAsync(key(), entryProcReader.getEntryProcessor(), entryProcReader.getArgs(isKeepBinary())),
            v -> new ClientObjectResponse(requestId(), v)).chain(f -> {
                try {
                    return f.get();
                }
                catch (Exception e) {
                    Exception e0 = U.unwrap(e);

                    if (X.hasCause(e0, EntryProcessorException.class))
                        throw new IgniteClientException(ClientStatus.ENTRY_PROCESSOR_EXCEPTION, e0.getMessage(), e0);
                    else
                        throw new GridClosureException(e0);
                }
            });
    }

    /** Helper class to read entry processor and it's arguments. */
    public static class EntryProcessorReader {
        /** */
        private final Object entryProc;

        /** */
        private final byte entryProcPlatform;

        /** */
        private final Object[] args;

        /** Objects reader. */
        private final BinaryReaderEx reader;

        /** */
        private final int argsStartPos;

        /** */
        public EntryProcessorReader(BinaryReaderEx reader) {
            entryProc = reader.readObjectDetached();
            entryProcPlatform = reader.readByte();

            int argCnt = reader.readInt();

            // We can't deserialize some types (arrays of user defined types for example) from detached objects.
            // On the other hand, deserialize should be done as part of process() call (not in constructor) for proper
            // error handling.
            // To overcome these issues we store binary reader reference, parse request in constructor (by reading detached
            // objects), restore arguments starting position in input stream and deserialize arguments from input stream
            // in process() method.
            this.reader = reader;
            argsStartPos = reader.in().position();

            args = new Object[argCnt];

            for (int i = 0; i < argCnt; i++)
                args[i] = reader.readObjectDetached();
        }

        /** */
        public EntryProcessor<Object, Object, Object> getEntryProcessor() {
            if (!(entryProc instanceof BinaryObject)) {
                throw new IgniteClientException(ClientStatus.FAILED,
                    "Entry processor should be marshalled as a BinaryObject: " + entryProc.getClass());
            }

            BinaryObjectImpl bo = (BinaryObjectImpl)entryProc;

            if (entryProcPlatform == ClientPlatform.JAVA)
                return bo.deserialize();

            throw new IgniteClientException(ClientStatus.FAILED, "Unsupported entry processor platform: " +
                entryProcPlatform);
        }

        /** */
        public Object[] getArgs(boolean keepBinary) {
            reader.in().position(argsStartPos);

            // Deserialize entry processor's arguments when not in keepBinary mode.
            if (!keepBinary && args.length > 0) {
                for (int i = 0; i < args.length; i++)
                    args[i] = reader.readObject();
            }

            return args;
        }
    }
}
