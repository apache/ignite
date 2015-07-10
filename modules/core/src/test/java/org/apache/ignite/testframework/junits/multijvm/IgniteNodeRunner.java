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

package org.apache.ignite.testframework.junits.multijvm;

import com.thoughtworks.xstream.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import sun.jvmstat.monitor.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * Run ignite node.
 */
public class IgniteNodeRunner {
    /** */
    private static final String IGNITE_CONFIGURATION_FILE = System.getProperty("java.io.tmpdir") +
        File.separator + "igniteConfiguration.tmp_";

    /** */
    private static volatile Ignite ignite;

    /**
     * Starts {@link Ignite} instance accorging to given arguments.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        X.println("Starting Ignite Node... Args=" + Arrays.toString(args));

        IgniteConfiguration cfg = readCfgFromFileAndDeleteFile(args[0]);

        ignite = Ignition.start(cfg);
    }

    /**
     * @return Ignite instance started at main.
     */
    public static IgniteEx startedInstance(){
        return (IgniteEx)ignite;
    }

    /**
     * @return <code>True</code> if there is ignite node started via {@link IgniteNodeRunner} at this JVM.
     */
    public static boolean hasStartedInstance() {
        return ignite != null;
    }

    /**
     * Stores {@link IgniteConfiguration} to file as xml.
     *
     * @param cfg Ignite Configuration.
     * @return A name of file where the configuration was stored.
     * @throws IOException If failed.
     * @see #readCfgFromFileAndDeleteFile(String)
     */
    public static String storeToFile(IgniteConfiguration cfg) throws IOException {
        String fileName = IGNITE_CONFIGURATION_FILE + cfg.getNodeId();

        // Check marshaller configuration, because read configuration method expect specific marshaller.
        if (cfg.getMarshaller() instanceof OptimizedMarshaller){
            OptimizedMarshaller marsh = (OptimizedMarshaller)cfg.getMarshaller();

            try {
                Field isRequireFiled = marsh.getClass().getDeclaredField("requireSer");

                isRequireFiled.setAccessible(true);

                boolean isRequireSer = isRequireFiled.getBoolean(marsh);

                if (isRequireSer)
                    throw new UnsupportedOperationException("Unsupported marshaller configuration. " +
                        "readCfgFromFileAndDeleteFile method expect " + OptimizedMarshaller.class.getSimpleName() +
                        "with requireSerializeble flag in 'false'.");
            }
            catch (NoSuchFieldException|IllegalAccessException e) {
                throw new IgniteException("Failed to check filed of " + OptimizedMarshaller.class.getSimpleName(), e);
            }
        }
        else
            throw new UnsupportedOperationException("Unsupported marshaller. " +
                "readCfgFromFileAndDeleteFile method expect " + OptimizedMarshaller.class.getSimpleName());

        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName))) {
            cfg.setMBeanServer(null);
            cfg.setMarshaller(null);
            cfg.setDiscoverySpi(null);
            cfg.setGridLogger(null);

            new XStream().toXML(cfg, out);
        }

        return fileName;
    }

    /**
     * Reads configuration from given file and delete the file after.
     *
     * @param fileName File name.
     * @return Readed configuration.
     * @throws IOException If failed.
     * @see #storeToFile(IgniteConfiguration)
     */
    private static IgniteConfiguration readCfgFromFileAndDeleteFile(String fileName) throws IOException {
        try(BufferedReader cfgReader = new BufferedReader(new FileReader(fileName))) {
            IgniteConfiguration cfg = (IgniteConfiguration)new XStream().fromXML(cfgReader);

            cfg.setMarshaller(new OptimizedMarshaller(false));

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);
            cfg.setDiscoverySpi(disco);

            return cfg;
        }
        finally {
            new File(fileName).delete();
        }
    }

    /**
     * Kill all Jvm runned by {#link IgniteNodeRunner}. Works based on jps command.
     *
     * @return List of killed process ids.
     * @throws Exception If exception.
     */
    public static List<Integer> killAll() throws Exception{
        MonitoredHost monitoredHost = MonitoredHost.getMonitoredHost(new HostIdentifier("localhost"));

        Set<Integer> jvms = monitoredHost.activeVms();

        List<Integer> res = new ArrayList<>();

        for (Integer jvmId : jvms) {
            try {
                MonitoredVm vm = monitoredHost.getMonitoredVm(new VmIdentifier("//" + jvmId + "?mode=r"), 0);

                if (IgniteNodeRunner.class.getName().equals(MonitoredVmUtil.mainClass(vm, true))) {
                    Process killProc = U.isWindows() ?
                        Runtime.getRuntime().exec(new String[] {"taskkill", "/pid", jvmId.toString(), "/f", "/t"}) :
                        Runtime.getRuntime().exec(new String[] {"kill", "-9", jvmId.toString()});

                    killProc.waitFor();

                    res.add(jvmId);
                }
            }
            catch (Exception e) {
                // Print stack trace just for information.
                X.printerrln("Could not kill IgniteNodeRunner java processes. Jvm pid = " + jvmId, e);
            }
        }

        return res;
    }
}
