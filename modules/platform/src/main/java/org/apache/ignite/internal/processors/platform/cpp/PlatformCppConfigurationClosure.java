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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PlatformConfiguration;
import org.apache.ignite.internal.processors.platform.PlatformAbstractConfigurationClosure;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManagerImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.platform.cpp.PlatformCppConfiguration;

import java.util.Collections;

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
            igniteCfg.setMarshaller(new PortableMarshaller());

            cppCfg0.warnings(Collections.singleton("Marshaller is automatically set to " +
                PortableMarshaller.class.getName() + " (other nodes must have the same marshaller type)."));
        }
        else if (!(marsh instanceof PortableMarshaller))
            throw new IgniteException("Unsupported marshaller (only " + PortableMarshaller.class.getName() +
                " can be used when running Ignite for C++): " + marsh.getClass().getName());

        // Set Ignite home so that marshaller context works.
        String ggHome = igniteCfg.getIgniteHome();

        if (ggHome == null)
            ggHome = U.getIgniteHome();
        else
            // If user provided IGNITE_HOME - set it as a system property.
            U.setIgniteHome(ggHome);

        try {
            U.setWorkDirectory(igniteCfg.getWorkDirectory(), ggHome);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}