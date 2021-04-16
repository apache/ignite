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

package org.apache.ignite.internal.processors.platform.cpp;

import java.util.Collections;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PlatformConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.platform.PlatformAbstractConfigurationClosure;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManagerImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.platform.cpp.PlatformCppConfiguration;

/**
 * Interop CPP configuration closure.
 */
public class PlatformCppConfigurationClosure extends PlatformAbstractConfigurationClosure {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param envPtr Environment pointer.
     */
    public PlatformCppConfigurationClosure(long envPtr) {
        super(envPtr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected void apply0(IgniteConfiguration igniteCfg) {
        // 3. Validate and copy Interop configuration setting environment pointer along the way.
        PlatformConfiguration interopCfg = igniteCfg.getPlatformConfiguration();

        if (interopCfg != null && !(interopCfg instanceof PlatformCppConfiguration))
            throw new IgniteException("Illegal interop configuration (must be of type " +
                PlatformCppConfiguration.class.getName() + "): " + interopCfg.getClass().getName());

        PlatformCppConfiguration cppCfg = interopCfg != null ? (PlatformCppConfiguration)interopCfg : null;

        if (cppCfg == null)
            cppCfg = new PlatformCppConfiguration();

        PlatformMemoryManagerImpl memMgr = new PlatformMemoryManagerImpl(gate, 1024);

        PlatformCppConfigurationEx cppCfg0 = new PlatformCppConfigurationEx(cppCfg, gate, memMgr);

        igniteCfg.setPlatformConfiguration(cppCfg0);

        // Check marshaller
        Marshaller marsh = igniteCfg.getMarshaller();

        if (marsh == null) {
            igniteCfg.setMarshaller(new BinaryMarshaller());

            cppCfg0.warnings(Collections.singleton("Marshaller is automatically set to " +
                BinaryMarshaller.class.getName() + " (other nodes must have the same marshaller type)."));
        }
        else if (!(marsh instanceof BinaryMarshaller))
            throw new IgniteException("Unsupported marshaller (only " + BinaryMarshaller.class.getName() +
                " can be used when running Apache Ignite C++): " + marsh.getClass().getName());

        BinaryConfiguration bCfg = igniteCfg.getBinaryConfiguration();

        if (bCfg == null) {
            bCfg = new BinaryConfiguration();

            bCfg.setCompactFooter(false);
            bCfg.setNameMapper(new BinaryBasicNameMapper(true));
            bCfg.setIdMapper(new BinaryBasicIdMapper(true));

            igniteCfg.setBinaryConfiguration(bCfg);

            cppCfg0.warnings(Collections.singleton("Binary configuration is automatically initiated, " +
                "note that binary name mapper is set to " + bCfg.getNameMapper()
                + " and binary ID mapper is set to " + bCfg.getIdMapper()
                + " (other nodes must have the same binary name and ID mapper types)."));
        }
        else {
            BinaryNameMapper nameMapper = bCfg.getNameMapper();

            if (nameMapper == null) {
                bCfg.setNameMapper(new BinaryBasicNameMapper(true));

                cppCfg0.warnings(Collections.singleton("Binary name mapper is automatically set to " +
                    bCfg.getNameMapper()
                    + " (other nodes must have the same binary name mapper type)."));
            }

            BinaryIdMapper idMapper = bCfg.getIdMapper();

            if (idMapper == null) {
                bCfg.setIdMapper(new BinaryBasicIdMapper(true));

                cppCfg0.warnings(Collections.singleton("Binary ID mapper is automatically set to " +
                    bCfg.getIdMapper()
                    + " (other nodes must have the same binary ID mapper type)."));
            }
        }

        if (bCfg.isCompactFooter())
            throw new IgniteException("Unsupported " + BinaryMarshaller.class.getName() +
                " \"compactFooter\" flag: must be false when running Apache Ignite C++.");

        // Set Ignite home so that marshaller context works.
        String ggHome = igniteCfg.getIgniteHome();

        if (ggHome != null)
            U.setIgniteHome(ggHome);
    }
}
