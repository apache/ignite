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
package java.net;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;

/** */
public class DnsBlocker {
    /** */
    private static final String BLOCK_DNS_FILE = "/tmp/block_dns";

    /** */
    public static final DnsBlocker INSTANCE = new DnsBlocker();

    /** */
    private DnsBlocker() {
        // No-op.
    }

    /**
     * Check and block hostname resolve request if needed.
     * @param impl Implementation.
     * @param hostname Hostname.
     */
    public void onHostResolve(InetAddressImpl impl, String hostname) throws UnknownHostException {
        if (!impl.loopbackAddress().getHostAddress().equals(hostname))
            check(hostname);
    }

    /**
     * Check and block address resolve request if needed.
     * @param impl Implementation.
     * @param addr Address.
     */
    public void onAddrResolve(InetAddressImpl impl, byte[] addr) throws UnknownHostException {
        if (!Arrays.equals(impl.loopbackAddress().getAddress(), addr))
            check(InetAddress.getByAddress(addr).toString());
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
