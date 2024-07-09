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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.startup.cmdline.CommandLineStartup;
import sun.net.spi.nameservice.NameService;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

/** */
public class DnsBlocker implements NameService {
    /** */
    private static final String BLOCK_DNS_FILE = "/tmp/block_dns";

    /** */
    private final InetAddress loopback;

    /** Original NameService to use after unblock. */
    private final NameService orig;

    /**
     * @param orig Original NameService to use after unblock.
     */
    private DnsBlocker(NameService orig) {
        loopback = InetAddress.getLoopbackAddress();
        this.orig = orig;
    }

    /** Installs DnsBlocker as main NameService to JVM. */
    private static void install() throws IgniteCheckedException {
        List<NameService> nameSrvc = U.staticField(InetAddress.class, "nameServices");

        NameService ns = new DnsBlocker(nameSrvc.get(0));

        // Put the blocking name service ahead.
        nameSrvc.add(0, ns);

        System.out.println("Installed DnsBlocker as main NameService to JVM [ns=" + nameSrvc.size() + ']');
    }

    /** */
    public static void main(String[] args) throws Exception {
        install();

        CommandLineStartup.main(args);
    }

    /** */
    @Override public InetAddress[] lookupAllHostAddr(String hostname) throws UnknownHostException {
        if (!loopback.getHostAddress().equals(hostname))
            check(hostname);

        return orig.lookupAllHostAddr(hostname);
    }

    /** */
    @Override public String getHostByAddr(byte[] addr) throws UnknownHostException {
        if (!Arrays.equals(loopback.getAddress(), addr))
            check(InetAddress.getByAddress(addr).toString());

        return orig.getHostByAddr(addr);
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
