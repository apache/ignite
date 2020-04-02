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

package org.apache.ignite.internal.processors.platform.dotnet;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PlatformConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.logger.platform.PlatformLogger;
import org.apache.ignite.internal.processors.platform.PlatformAbstractConfigurationClosure;
import org.apache.ignite.internal.processors.platform.lifecycle.PlatformLifecycleBean;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManagerImpl;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.platform.dotnet.PlatformDotNetAffinityFunction;
import org.apache.ignite.platform.dotnet.PlatformDotNetConfiguration;
import org.apache.ignite.platform.dotnet.PlatformDotNetLifecycleBean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.DEFAULT_PROTOCOL_CONTEXT;

/**
 * Closure to apply dot net configuration.
 */
public class PlatformDotNetConfigurationClosure extends PlatformAbstractConfigurationClosure {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether to use platform logger (when custom logger is defined on .NET side). */
    private final boolean useLogger;

    /** Configuration. */
    private IgniteConfiguration cfg;

    /** Memory manager. */
    private PlatformMemoryManagerImpl memMgr;

    /**
     * Constructor.
     *
     * @param envPtr Environment pointer.
     */
    public PlatformDotNetConfigurationClosure(long envPtr, boolean useLogger) {
        super(envPtr);

        this.useLogger = useLogger;
    }

    /** {@inheritDoc} */
    @Override protected void apply0(IgniteConfiguration igniteCfg) {
        // Validate and copy Interop configuration setting environment pointer along the way.
        PlatformConfiguration interopCfg = igniteCfg.getPlatformConfiguration();

        if (interopCfg != null && !(interopCfg instanceof PlatformDotNetConfiguration))
            throw new IgniteException("Illegal platform configuration (must be of type " +
                PlatformDotNetConfiguration.class.getName() + "): " + interopCfg.getClass().getName());

        PlatformDotNetConfiguration dotNetCfg = interopCfg != null ? (PlatformDotNetConfiguration)interopCfg : null;

        if (dotNetCfg == null)
            dotNetCfg = new PlatformDotNetConfiguration();

        memMgr = new PlatformMemoryManagerImpl(gate, 1024);

        PlatformLogger logger = null;

        if (useLogger) {
            logger = new PlatformLogger();
            logger.setGateway(gate);
            igniteCfg.setGridLogger(logger);
        }

        PlatformDotNetConfigurationEx dotNetCfg0 = new PlatformDotNetConfigurationEx(dotNetCfg, gate, memMgr,
            logger);

        igniteCfg.setPlatformConfiguration(dotNetCfg0);

        // Set Ignite home so that marshaller context works.
        String ggHome = igniteCfg.getIgniteHome();

        if (ggHome != null)
            U.setIgniteHome(ggHome);

        // Callback to .Net.
        prepare(igniteCfg, dotNetCfg0);

        // Make sure binary config is right.
        setBinaryConfiguration(igniteCfg, dotNetCfg0);
    }

    /**
     * Sets binary config.
     *
     * @param igniteCfg Ignite config.
     * @param dotNetCfg .NET config.
     */
    private void setBinaryConfiguration(IgniteConfiguration igniteCfg, PlatformDotNetConfigurationEx dotNetCfg) {
        // Check marshaller.
        Marshaller marsh = igniteCfg.getMarshaller();

        if (marsh == null) {
            igniteCfg.setMarshaller(new BinaryMarshaller());

            dotNetCfg.warnings(Collections.singleton("Marshaller is automatically set to " +
                BinaryMarshaller.class.getName() + " (other nodes must have the same marshaller type)."));
        }
        else if (!(marsh instanceof BinaryMarshaller))
            throw new IgniteException("Unsupported marshaller (only " + BinaryMarshaller.class.getName() +
                " can be used when running Apache Ignite.NET): " + marsh.getClass().getName());

        BinaryConfiguration bCfg = igniteCfg.getBinaryConfiguration();
    }

    /**
     * Prepare .Net size.
     *
     * @param igniteCfg Ignite configuration.
     * @param interopCfg Interop configuration.
     */
    private void prepare(IgniteConfiguration igniteCfg, PlatformDotNetConfigurationEx interopCfg) {
        cfg = igniteCfg;

        try (PlatformMemory outMem = memMgr.allocate()) {
            try (PlatformMemory inMem = memMgr.allocate()) {
                PlatformOutputStream out = outMem.output();

                GridBinaryMarshaller marshaller = PlatformUtils.marshaller();
                BinaryRawWriterEx writer = marshaller.writer(out);

                PlatformConfigurationUtils.writeDotNetConfiguration(writer, interopCfg.unwrap());

                // Write .NET beans
                List<PlatformDotNetLifecycleBean> beans = beans(igniteCfg);

                writer.writeInt(beans.size());

                for (PlatformDotNetLifecycleBean bean : beans) {
                    writer.writeString(bean.getTypeName());
                    writer.writeMap(bean.getProperties());
                }

                // Write .NET affinity functions
                List<PlatformDotNetAffinityFunction> affFuncs = affinityFunctions(igniteCfg);

                writer.writeInt(affFuncs.size());

                for (PlatformDotNetAffinityFunction func : affFuncs) {
                    writer.writeString(func.getTypeName());
                    writer.writeMap(func.getProperties());
                }

                out.synchronize();

                gate.extensionCallbackInLongLongOutLong(
                    PlatformUtils.OP_PREPARE_DOT_NET, outMem.pointer(), inMem.pointer());

                processPrepareResult(marshaller.reader(inMem.input()));
            }
        }
    }

    /**
     * Process prepare result.
     *
     * @param in Input stream.
     */
    private void processPrepareResult(BinaryReaderExImpl in) {
        assert cfg != null;

        PlatformConfigurationUtils.readIgniteConfiguration(in, cfg, DEFAULT_PROTOCOL_CONTEXT);

        // Process beans
        List<PlatformDotNetLifecycleBean> beans = beans(cfg);
        List<PlatformLifecycleBean> newBeans = new ArrayList<>();

        int len = in.readInt();

        for (int i = 0; i < len; i++) {
            if (i < beans.size())
                // Existing bean.
                beans.get(i).initialize(gate, in.readLong());
            else
                // This bean is defined in .Net.
                newBeans.add(new PlatformLifecycleBean(gate, in.readLong()));
        }

        if (!newBeans.isEmpty()) {
            LifecycleBean[] newBeans0 = newBeans.toArray(new LifecycleBean[newBeans.size()]);

            // New beans were added. Let's append them to the tail of the rest configured lifecycle beans.
            LifecycleBean[] oldBeans = cfg.getLifecycleBeans();

            if (oldBeans == null)
                cfg.setLifecycleBeans(newBeans0);
            else {
                LifecycleBean[] mergedBeans = new LifecycleBean[oldBeans.length + newBeans.size()];

                System.arraycopy(oldBeans, 0, mergedBeans, 0, oldBeans.length);
                System.arraycopy(newBeans0, 0, mergedBeans, oldBeans.length, newBeans0.length);

                cfg.setLifecycleBeans(mergedBeans);
            }
        }

        // Process affinity functions
        List<PlatformDotNetAffinityFunction> affFuncs = affinityFunctions(cfg);

        if (!affFuncs.isEmpty()) {
            for (PlatformDotNetAffinityFunction aff : affFuncs)
                aff.init(PlatformConfigurationUtils.readAffinityFunction(in));
        }
    }

    /**
     * Find .Net lifecycle beans in configuration.
     *
     * @param cfg Configuration.
     * @return Beans.
     */
    private static List<PlatformDotNetLifecycleBean> beans(IgniteConfiguration cfg) {
        List<PlatformDotNetLifecycleBean> res = new ArrayList<>();

        if (cfg.getLifecycleBeans() != null) {
            for (LifecycleBean bean : cfg.getLifecycleBeans()) {
                if (bean instanceof PlatformDotNetLifecycleBean)
                    res.add((PlatformDotNetLifecycleBean)bean);
            }
        }

        return res;
    }

    /**
     * Find .NET affinity functions in configuration.
     *
     * @param cfg Configuration.
     * @return affinity functions.
     */
    private static List<PlatformDotNetAffinityFunction> affinityFunctions(IgniteConfiguration cfg) {
        List<PlatformDotNetAffinityFunction> res = new ArrayList<>();

        CacheConfiguration[] cacheCfg = cfg.getCacheConfiguration();

        if (cacheCfg != null) {
            for (CacheConfiguration ccfg : cacheCfg) {
                if (ccfg.getAffinity() instanceof PlatformDotNetAffinityFunction)
                    res.add((PlatformDotNetAffinityFunction)ccfg.getAffinity());
            }
        }

        return res;
    }
}
