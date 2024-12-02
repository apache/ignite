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
package org.apache.ignite.internal.ducktest.tests.dns_failure_test;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.startup.cmdline.CommandLineStartup;

/** */
public class BlockingNameService implements NameServiceHandler {
    /** */
    private static final String BLOCK_DNS_FILE = "/tmp/block_dns";

    /** Private {@code NameService} class to proxy. */
    private static Class<?> nameSrvcCls;

    /** */
    private final InetAddress loopback;

    /** Original {@code NameService}. */
    private final Object origNameSrvc;

    /**
     * @param origNameSrvc Original {@code NameService}.
     */
    private BlockingNameService(Object origNameSrvc) {
        loopback = InetAddress.getLoopbackAddress();
        this.origNameSrvc = origNameSrvc;
    }

    /** Installs {@code BlockingNameService} as main {@code NameService} to JVM11. */
    private static void installJdk11() throws Exception {
        Field nameSrvcFld = InetAddress.class.getDeclaredField("nameService");
        nameSrvcFld.setAccessible(true);

        nameSrvcCls = Class.forName("java.net.InetAddress$NameService");

        BlockingNameService blkNameSrvc = new BlockingNameService(nameSrvcFld.get(InetAddress.class));

        nameSrvcFld.set(
            InetAddress.class,
            Proxy.newProxyInstance(nameSrvcCls.getClassLoader(), new Class<?>[] { nameSrvcCls }, blkNameSrvc));

        System.out.println("Installed DnsBlocker as main NameService to JVM");
    }

    /** */
    public static void main(String[] args) throws Exception {
        String jdkVer = U.jdkVersion();

        if ("11".equals(jdkVer) || "17".equals(jdkVer))
            installJdk11();
        else
            throw new IllegalArgumentException("Unsupported JDK version: " + jdkVer);

        System.out.println("Installed BlockingNameService as main NameService to JVM");

        CommandLineStartup.main(args);
    }

    /** */
    @Override public InetAddress[] lookupAllHostAddr(String hostname) throws UnknownHostException {
        if (!loopback.getHostAddress().equals(hostname))
            check(hostname);

        try {
            Method lookupAllHostAddr = nameSrvcCls.getDeclaredMethod("lookupAllHostAddr", String.class);
            lookupAllHostAddr.setAccessible(true);

            return (InetAddress[])lookupAllHostAddr.invoke(origNameSrvc, hostname);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    @Override public String getHostByAddr(byte[] addr) throws UnknownHostException {
        if (!Arrays.equals(loopback.getAddress(), addr))
            check(InetAddress.getByAddress(addr).toString());

        try {
            Method getHostByAddr = nameSrvcCls.getDeclaredMethod("getHostByAddr", byte[].class);
            getHostByAddr.setAccessible(true);

            return (String)getHostByAddr.invoke(origNameSrvc, addr);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private void check(String req) throws UnknownHostException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        File file = new File(BLOCK_DNS_FILE);

        if (file.exists()) {
            try {
                Scanner scanner = new Scanner(file);
                if (!scanner.hasNextLong())
                    throw new RuntimeException("Wrong " + BLOCK_DNS_FILE + " file format");

                long timeout = scanner.nextLong();

                if (!scanner.hasNextBoolean())
                    throw new RuntimeException("Wrong " + BLOCK_DNS_FILE + " file format");

                boolean fail = scanner.nextBoolean();

                // Can't use logger here, because class need to be in bootstrap classloader.
                System.out.println(sdf.format(new Date()) + " [" + Thread.currentThread().getName() +
                    "] DNS request " + req + " blocked for " + timeout + " ms");

                Thread.dumpStack();

                Thread.sleep(timeout);

                if (fail)
                    throw new UnknownHostException();
            }
            catch (InterruptedException | FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            System.out.println(sdf.format(new Date()) + " [" + Thread.currentThread().getName() +
                "] Passed DNS request " + req);

            Thread.dumpStack();
        }
    }
}
