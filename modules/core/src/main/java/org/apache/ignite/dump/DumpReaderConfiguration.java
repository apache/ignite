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

package org.apache.ignite.dump;

import java.io.File;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Configuration class of {@link IgniteDumpReader}.
 *
 * @see IgniteDumpReader
 * @see DumpConsumer
 */
@IgniteExperimental
public class DumpReaderConfiguration {
    /** Root dump directory. */
    private final File dir;

    /** Dump consumer. */
    private final DumpConsumer cnsmr;

    /** Count of threads to consume dumped partitions. */
    private final int thCnt;

    /**
     * @param dir Root dump directory.
     * @param cnsmr Dump consumer.
     * @param thCnt Count of threads to consume dumped partitions.
     */
    public DumpReaderConfiguration(File dir, DumpConsumer cnsmr, int thCnt) {
        this.dir = dir;
        this.cnsmr = cnsmr;
        this.thCnt = thCnt;
    }

    /** @return Root dump directiory. */
    public File dumpRoot() {
        return dir;
    }

    /** @return Dump consumer instance. */
    public DumpConsumer consumer() {
        return cnsmr;
    }

    /** @return Count of threads to consume dumped partitions. */
    public int threadCount() {
        return thCnt;
    }
}
