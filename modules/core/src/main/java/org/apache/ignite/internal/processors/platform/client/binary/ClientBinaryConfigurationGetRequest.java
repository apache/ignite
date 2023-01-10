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

package org.apache.ignite.internal.processors.platform.client.binary;

import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.beforestart.BeforeStartupRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Binary configuration retrieval request.
 */
public class ClientBinaryConfigurationGetRequest extends ClientRequest implements BeforeStartupRequest {
    /** */
    private static final byte NAME_MAPPER_BASIC_FULL = 0;

    /** */
    private static final byte NAME_MAPPER_BASIC_SIMPLE = 1;

    /** */
    private static final byte NAME_MAPPER_CUSTOM = 2;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientBinaryConfigurationGetRequest(BinaryRawReader reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        BinaryConfiguration cfg = ctx.kernalContext().config().getBinaryConfiguration();
        boolean compactFooter = cfg == null ? BinaryConfiguration.DFLT_COMPACT_FOOTER : cfg.isCompactFooter();
        byte nameMapperType = getNameMapperType(cfg);

        return new ClientBinaryConfigurationGetResponse(requestId(), compactFooter, nameMapperType);
    }

    /**
     * Gets the binary name mapper type code.
     * @param cfg Binary configuration.
     *
     * @return Mapper type code.
     */
    private static byte getNameMapperType(@Nullable BinaryConfiguration cfg) {
        if (cfg == null || cfg.getNameMapper() == null)
            return BinaryBasicNameMapper.DFLT_SIMPLE_NAME ? NAME_MAPPER_BASIC_SIMPLE : NAME_MAPPER_BASIC_FULL;

        if (!cfg.getNameMapper().getClass().equals(BinaryBasicNameMapper.class))
            return NAME_MAPPER_CUSTOM;

        BinaryBasicNameMapper basicNameMapper = (BinaryBasicNameMapper)cfg.getNameMapper();

        return basicNameMapper.isSimpleName() ? NAME_MAPPER_BASIC_SIMPLE : NAME_MAPPER_BASIC_FULL;
    }
}
