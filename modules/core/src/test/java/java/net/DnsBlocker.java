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

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/** */
public class DnsBlocker {
    /** */
    private static final long BLOCK_TIMOUT = 30_000L;

    /** */
    public static final DnsBlocker INSTANCE = new DnsBlocker();

    /** */
    private volatile boolean blocked = true;

    /** */
    private DnsBlocker() {
        // No-op.
    }

    /**
     * Check and block DNS request if needed.
     *
     * @param req Request description.
     */
    public void check(String req) {
        if (blocked) {
            try {
                // Can't use logger here, because class need to be in bootstrap classloader.
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                System.out.println(sdf.format(new Date()) + " [" + Thread.currentThread().getName() +
                    "] DNS request " + req + " blocked for " + BLOCK_TIMOUT + " ms");

                Thread.sleep(BLOCK_TIMOUT);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Block DNS requests. */
    public void block() {
        blocked = true;
    }

    /** Unblock DNS requests. */
    public void unblock() {
        blocked = false;
    }
}
