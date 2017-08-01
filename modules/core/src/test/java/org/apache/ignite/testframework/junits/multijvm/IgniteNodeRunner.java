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

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.mapper.CannotResolveClassException;
import com.thoughtworks.xstream.mapper.MapperWrapper;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import sun.jvmstat.monitor.HostIdentifier;
import sun.jvmstat.monitor.MonitoredHost;
import sun.jvmstat.monitor.MonitoredVm;
import sun.jvmstat.monitor.MonitoredVmUtil;
import sun.jvmstat.monitor.VmIdentifier;

/**
 * Run ignite node.
 */
public class IgniteNodeRunner {
    /** */
    private static final String IGNITE_CONFIGURATION_FILE = System.getProperty("java.io.tmpdir") +
        File.separator + "igniteConfiguration.tmp_";

    /** */
    private static final String IGNITE_CONFIGURATION_CLOSURE_POSTFIX = "_closure";

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

        IgniteConfiguration cfg = null;

        if (args.length > 1)
            cfg = readCfgFromFileAndDeleteFile(args[0], args[1]);
        else
            cfg = readCfgFromFileAndDeleteFile(args[0]);

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
    public static String storeToFile(IgniteConfiguration cfg, boolean resetDiscovery) throws IOException, IgniteCheckedException {
        String fileName = IGNITE_CONFIGURATION_FILE + cfg.getNodeId();

        storeToFile(cfg, fileName, true, resetDiscovery);

        return fileName;
    }

    /**
     * Stores {@link IgniteConfiguration} to file as xml.
     *
     * @param cfg Ignite Configuration.
     * @param fileName A name of file where the configuration was stored.
     * @param resetMarshaller Reset marshaller configuration to default.
     * @param resetDiscovery Reset discovery configuration to default.
     * @throws IOException If failed.
     * @see #readCfgFromFileAndDeleteFile(String)
     */
    public static void storeToFile(IgniteConfiguration cfg, String fileName,
        boolean resetMarshaller,
        boolean resetDiscovery) throws IOException, IgniteCheckedException {
        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName))) {
            IgniteConfiguration cfg0 = new IgniteConfiguration(cfg);

            if (resetMarshaller)
                cfg0.setMarshaller(null);

            if (resetDiscovery)
                cfg0.setDiscoverySpi(null);

            cfg0.setWorkDirectory(U.defaultWorkDirectory());
            cfg0.setMBeanServer(null);
            cfg0.setGridLogger(null);

            new XStream().toXML(cfg0, out);
        }
    }

    /**
     * Stores {@link IgniteInClosure} to file as xml.
     *
     * @param clos IgniteClosure.
     * @param cfgFileName A name of file where the configuration was stored.
     * @return A name of file where the given closure was stored.
     * @throws IOException If failed.
     */
    public static String storeToFile(IgniteInClosure clos, String cfgFileName) throws IOException {
        String fileName = cfgFileName + IGNITE_CONFIGURATION_CLOSURE_POSTFIX;

        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName))) {
            // TODO: serialize anonymous classes in proper way
            new XStream().toXML(clos, out);
        }

        return fileName;
    }

    /**
     * Reads configuration and closure from given files names,
     * applies the closure to the configuration
     * and delete the files after.
     *
     * @param cfgFileName Configuration file name.
     * @param closFileName Closure file name.
     * @return Read and prepared configuration.
     * @throws IOException If failed.
     * @throws IgniteCheckedException On error.
     * @see #storeToFile(IgniteConfiguration, boolean)
     * @see #storeToFile(IgniteInClosure, String)
     */
    @SuppressWarnings("unchecked")
    private static IgniteConfiguration readCfgFromFileAndDeleteFile(String cfgFileName, String closFileName)
        throws IOException, IgniteCheckedException {

        IgniteConfiguration cfg = readCfgFromFileAndDeleteFile(cfgFileName);

        try (BufferedReader closReader = new BufferedReader(new FileReader(closFileName))) {
            IgniteInClosure clos = (IgniteInClosure)createXStreamer().fromXML(closReader);

            clos.apply(cfg);
        }
        finally {
            new File(closFileName).delete();
        }

        return cfg;
    }

    /**
     * Reads configuration from given file and delete the file after.
     *
     * @param fileName File name.
     * @return Readed configuration.
     * @throws IOException If failed.
     * @throws IgniteCheckedException On error.
     * @see #storeToFile(IgniteConfiguration, boolean)
     */
    private static IgniteConfiguration readCfgFromFileAndDeleteFile(String fileName)
        throws IOException, IgniteCheckedException {
        try (BufferedReader cfgReader = new BufferedReader(new FileReader(fileName))) {

            IgniteConfiguration cfg = (IgniteConfiguration)createXStreamer().fromXML(cfgReader);

            if (cfg.getMarshaller() == null) {
                Marshaller marsh = IgniteTestResources.getMarshaller();

                cfg.setMarshaller(marsh);
            }

            X.println("Configured marshaller class: " + cfg.getMarshaller().getClass().getName());

            if (cfg.getDiscoverySpi() == null) {
                TcpDiscoverySpi disco = new TcpDiscoverySpi();
                disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);
                cfg.setDiscoverySpi(disco);
            }

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
                    Process killProc = Runtime.getRuntime().exec(U.isWindows() ?
                        new String[] {"taskkill", "/pid", jvmId.toString(), "/f", "/t"} :
                        new String[] {"kill", "-9", jvmId.toString()});

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

    /**
     * Creates an instance of {@link XStream} and configure it to ignore {@link CannotResolveClassException},
     * and to ignore unknown elements during deserialization.
     * It needs for deserialization of configuration classes on nodes with different build versions.
     *
     * @return Configured {@link XStream}.
     */
    private static XStream createXStreamer() {
        XStream xStream = new XStream() {
            @Override protected MapperWrapper wrapMapper(MapperWrapper next) {
                return new MapperWrapper(next) {
                    @Override public Class realClass(String elementName) {
                        try {
                            return super.realClass(elementName);
                        }
                        catch (CannotResolveClassException ignored) {
                            // in case of addition to configurations  a field with a new type of class
                            // and can't be deserialized because a definition is absent in classpath
                            return null;
                        }
                    }
                };
            }
        };

        xStream.ignoreUnknownElements(); // to avoid UnknownFieldException in MultiVersion mode

        return xStream;
    }
}
