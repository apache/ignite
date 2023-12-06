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

package org.apache.ignite.internal.management.cdc;

import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * The command to forcefully resend all cache data to CDC.
 * Iterates over given caches and writes data entries to the WAL to get captured by CDC.
 */
@IgniteExperimental
public class CdcResendCommand implements ComputeCommand<CdcResendCommandArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Forcefully resend all cache data to CDC. " +
            "Iterates over caches and writes primary copies of data entries to the WAL to get captured by CDC";
    }

    /** {@inheritDoc} */
    @Override public Class<CdcResendCommandArg> argClass() {
        return CdcResendCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<CdcCacheDataResendTask> taskClass() {
        return CdcCacheDataResendTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(CdcResendCommandArg arg, Void res, Consumer<String> printer) {
        printer.accept("Successfully resent all cache data to CDC.");
    }
}
