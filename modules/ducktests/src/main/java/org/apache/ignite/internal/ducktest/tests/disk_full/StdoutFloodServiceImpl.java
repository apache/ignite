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

package org.apache.ignite.internal.ducktest.tests.disk_full;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.resources.ServiceContextResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * Writes 1 KB lines to {@code System.out} at about 10 MB/s until cancelled.
 */
public class StdoutFloodServiceImpl implements Service, StdoutFloodService {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Line, 1 KB with its line separator. */
    private static final String LINE = "x".repeat(1023);

    /** Lines written per millisecond. */
    private static final int LINES_PER_MS = 10;

    /** Service context. */
    @ServiceContextResource
    private ServiceContext ctx;

    /** Lines written. */
    private final AtomicLong lines = new AtomicLong();

    /** {@inheritDoc} */
    @Override public void execute() throws Exception {
        while (!ctx.isCancelled()) {
            for (int i = 0; i < LINES_PER_MS; i++) {
                System.out.println(LINE);

                lines.incrementAndGet();
            }

            Thread.sleep(1);
        }
    }

    /** {@inheritDoc} */
    @Override public long lines() {
        return lines.get();
    }
}
