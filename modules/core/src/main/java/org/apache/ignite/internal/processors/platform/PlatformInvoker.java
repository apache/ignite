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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;

/**
 * Invokes platform operations.
 */
public class PlatformInvoker {
    /**
     * Invokes an operation with specified code.
     *
     * @param platformCtx Platform context.
     * @param opCode Operation code.
     * @param memPtr Pointer to a stream with data.
     */
    public static long invoke(PlatformContext platformCtx, int opCode, long memPtr) {
        final PlatformMemory mem = platformCtx.memory().get(memPtr);
        final BinaryRawReaderEx reader = platformCtx.reader(mem);

        switch (opCode)
        {
            default:
                throw new IgniteException("Unexpected opcode in platformInvoke: " + opCode);
        }
    }
}
