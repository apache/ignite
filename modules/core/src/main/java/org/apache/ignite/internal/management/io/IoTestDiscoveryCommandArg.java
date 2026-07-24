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

package org.apache.ignite.internal.management.io;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.util.typedef.internal.A;

/** Arguments of the discovery IO test. */
public class IoTestDiscoveryCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** Maximum payload size. */
    private static final int MAX_PAYLOAD_SIZE = 64 * 1024;

    /** Maximum number of samples. */
    private static final int MAX_SAMPLES = 100;

    /** Minimum interval between samples. */
    private static final long MIN_INTERVAL = 10;

    /** Maximum interval between samples. */
    private static final long MAX_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    /** */
    @Order(0)
    @Argument(optional = true, description = "Number of samples (max 100).")
    int samples = 10;

    /** */
    @Order(1)
    @Argument(optional = true, description = "Interval between samples (millis, 10 to 60000).")
    long interval = 100;

    /** */
    @Order(2)
    @Argument(optional = true, description = "Payload size (bytes, max 64 KiB).")
    int payloadSize = 100;

    /** */
    public int samples() {
        return samples;
    }

    /** */
    public void samples(int samples) {
        A.ensure(samples > 0 && samples <= MAX_SAMPLES,
            "samples must be between 1 and " + MAX_SAMPLES);

        this.samples = samples;
    }

    /** */
    public long interval() {
        return interval;
    }

    /** */
    public void interval(long interval) {
        A.ensure(interval >= MIN_INTERVAL && interval <= MAX_INTERVAL,
            "interval must be between " + MIN_INTERVAL + " and " + MAX_INTERVAL);

        this.interval = interval;
    }

    /** */
    public int payloadSize() {
        return payloadSize;
    }

    /** */
    public void payloadSize(int payloadSize) {
        A.ensure(payloadSize >= 0 && payloadSize <= MAX_PAYLOAD_SIZE,
            "payloadSize must be between 0 and " + MAX_PAYLOAD_SIZE);

        this.payloadSize = payloadSize;
    }
}
