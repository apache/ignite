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

package org.apache.ignite.internal.cdc;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;

/**
 * Methods to reuse various CDC like utilities.
 */
public class CdcUtils {
    /**
     * Register {@code meta}.
     *
     * @param ctx Binary context.
     * @param log Logger.
     * @param meta Binary metadata to register.
     */
    public static void registerBinaryMeta(BinaryContext ctx, IgniteLogger log, BinaryMetadata meta) {
        ctx.updateMetadata(meta.typeId(), meta, false);

        if (log.isInfoEnabled())
            log.info("BinaryMeta [meta=" + meta + ']');
    }

    /**
     * Register {@code mapping}.
     *
     * @param ctx Binary context.
     * @param log Logger.
     * @param mapping Type mapping to register.
     */
    public static void registerMapping(BinaryContext ctx, IgniteLogger log, TypeMapping mapping) {
        assert mapping.platformType().ordinal() <= Byte.MAX_VALUE;

        byte platformType = (byte)mapping.platformType().ordinal();

        ctx.registerUserClassName(mapping.typeId(), mapping.typeName(), false, false, platformType);

        if (log.isInfoEnabled())
            log.info("Mapping [mapping=" + mapping + ']');
    }
}
