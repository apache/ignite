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

package org.apache.ignite.testframework.junits;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.persistence.filename.SharedFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/** */
public class GridTestBinaryMarshaller {
    /** */
    private final BinaryMarshaller marsh;

    /**
     * Default constructor.
     */
    public GridTestBinaryMarshaller(IgniteLogger log) {
        try {
            marsh = createBinaryMarshaller(log);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param obj Object.
     */
    public BinaryObject marshal(Object obj) throws IgniteCheckedException {
        byte[] bytes = marsh.marshal(obj);

        return BinaryUtils.binariesFactory.binaryObject(U.<GridBinaryMarshaller>field(marsh, "impl").context(), bytes, 0);
    }

    /**
     * @param log Logger.
     */
    private BinaryMarshaller createBinaryMarshaller(IgniteLogger log) throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration()
            .setBinaryConfiguration(
                new BinaryConfiguration().setCompactFooter(true)
            )
            .setClientMode(false)
            .setDiscoverySpi(new TcpDiscoverySpi() {
                @Override public void sendCustomEvent(DiscoveryCustomMessage msg) throws IgniteException {
                    //No-op.
                }
            });

        MarshallerContextTestImpl marshCtx = new MarshallerContextTestImpl();

        marshCtx.setMarshallerMappingFileStoreDir(new SharedFileTree(U.defaultWorkDirectory()).marshaller());
        marshCtx.onMarshallerProcessorStarted(new GridTestKernalContext(log, iCfg), null);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(marshCtx);
        marsh.setBinaryContext(U.binaryContext(marsh, iCfg));

        return marsh;
    }
}
