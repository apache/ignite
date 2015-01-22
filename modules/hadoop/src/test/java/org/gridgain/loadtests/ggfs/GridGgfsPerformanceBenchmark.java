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

package org.gridgain.loadtests.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Tests write throughput.
 */
public class GridGgfsPerformanceBenchmark {
    /** Path to test hadoop configuration. */
    private static final String HADOOP_FS_CFG = "modules/core/src/test/config/hadoop/core-site.xml";

    /** FS prefix. */
    private static final String FS_PREFIX = "ggfs:///";

    /** Test writes. */
    private static final int OP_WRITE = 0;

    /** Test reads. */
    private static final int OP_READ = 1;

    /**
     * Starts benchmark.
     *
     * @param args Program arguments.
     *      [0] - number of threads, default 1.
     *      [1] - file length, default is 1GB.
     *      [2] - stream buffer size, default is 1M.
     *      [3] - fs config path.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final int threadNum = intArgument(args, 0, 1);
        final int op = intArgument(args, 1, OP_WRITE);
        final long fileLen = longArgument(args, 2, 256 * 1024 * 1024);
        final int bufSize = intArgument(args, 3, 128 * 1024);
        final String cfgPath = argument(args, 4, HADOOP_FS_CFG);
        final String fsPrefix = argument(args, 5, FS_PREFIX);
        final short replication = (short)intArgument(args, 6, 3);

        final Path ggfsHome = new Path(fsPrefix);

        final FileSystem fs = ggfs(ggfsHome, cfgPath);

        final AtomicLong progress = new AtomicLong();

        final AtomicInteger idx = new AtomicInteger();

        System.out.println("Warming up...");

//        warmUp(fs, ggfsHome, op, fileLen);

        System.out.println("Finished warm up.");

        if (op == OP_READ) {
            for (int i = 0; i < threadNum; i++)
                benchmarkWrite(fs, new Path(ggfsHome, "in-" + i), fileLen, bufSize, replication, null);
        }

        long total = 0;

        long start = System.currentTimeMillis();

        IgniteFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                String fileIdx = op == OP_READ ? String.valueOf(idx.getAndIncrement()) : UUID.randomUUID().toString();

                try {
                    for (int i = 0; i < 200; i++) {
                        if (op == OP_WRITE)
                            benchmarkWrite(fs, new Path(ggfsHome, "out-" + fileIdx), fileLen, bufSize, replication,
                                progress);
                        else
                            benchmarkRead(fs, new Path(ggfsHome, "in-" + fileIdx), bufSize, progress);
                    }

                    System.out.println("Finished " + (op == OP_WRITE ? "writing" : "reading") + " data.");
                }
                catch (Exception e) {
                    System.out.println("Failed to process stream: " + e);

                    e.printStackTrace();
                }
            }
        }, threadNum, "test-runner");

        while (!fut.isDone()) {
            U.sleep(1000);

            long written = progress.getAndSet(0);

            total += written;

            int mbytesPerSec = (int)(written / (1024 * 1024));

            System.out.println((op == OP_WRITE ? "Write" : "Read") + " rate [threads=" + threadNum +
                ", bufSize=" + bufSize + ", MBytes/s=" + mbytesPerSec + ']');
        }

        long now = System.currentTimeMillis();

        System.out.println((op == OP_WRITE ? "Written" : "Read") + " " + total + " bytes in " + (now - start) +
            "ms, avg write rate is " + (total * 1000 / ((now - start) * 1024 * 1024)) + "MBytes/s");

        fs.close();
    }

    /**
     * Warms up server side.
     *
     * @param fs File system.
     * @param ggfsHome GGFS home.
     * @throws Exception If failed.
     */
    private static void warmUp(FileSystem fs, Path ggfsHome, int op, long fileLen) throws Exception {
        Path file = new Path(ggfsHome, "out-0");

        benchmarkWrite(fs, file, fileLen, 1024 * 1024, (short)1, null);

        for (int i = 0; i < 5; i++) {
            if (op == OP_WRITE)
                benchmarkWrite(fs, file, fileLen, 1024 * 1024, (short)1, null);
            else
                benchmarkRead(fs, file, 1024 * 1024, null);
        }

        fs.delete(file, true);
    }

    /**
     * @param args Arguments.
     * @param idx Index.
     * @param dflt Default value.
     * @return Argument value.
     */
    private static int intArgument(String[] args, int idx, int dflt) {
        if (args.length <= idx)
            return dflt;

        try {
            return Integer.parseInt(args[idx]);
        }
        catch (NumberFormatException ignored) {
            return dflt;
        }
    }

    /**
     * @param args Arguments.
     * @param idx Index.
     * @param dflt Default value.
     * @return Argument value.
     */
    private static long longArgument(String[] args, int idx, long dflt) {
        if (args.length <= idx)
            return dflt;

        try {
            return Long.parseLong(args[idx]);
        }
        catch (NumberFormatException ignored) {
            return dflt;
        }
    }

    /**
     * @param args Arguments.
     * @param idx Index.
     * @param dflt Default value.
     * @return Argument value.
     */
    private static String argument(String[] args, int idx, String dflt) {
        if (args.length <= idx)
            return dflt;

        return args[idx];
    }

    /** {@inheritDoc} */
    private static FileSystem ggfs(Path home, String cfgPath) throws IOException {
        Configuration cfg = new Configuration();

        cfg.addResource(U.resolveGridGainUrl(cfgPath));

        return FileSystem.get(home.toUri(), cfg);
    }

    /**
     * Tests stream write to specified file.
     *
     * @param file File to write to.
     * @param len Length to write.
     * @param bufSize Buffer size.
     * @param replication Replication factor.
     * @param progress Progress that will be incremented on each written chunk.
     */
    private static void benchmarkWrite(FileSystem fs, Path file, long len, int bufSize, short replication,
        @Nullable AtomicLong progress) throws Exception {

        try (FSDataOutputStream out = fs.create(file, true, bufSize, replication, fs.getDefaultBlockSize())) {
            long written = 0;

            byte[] data = new byte[bufSize];

            while (written < len) {
                int chunk = (int) Math.min(len - written, bufSize);

                out.write(data, 0, chunk);

                written += chunk;

                if (progress != null)
                    progress.addAndGet(chunk);
            }

            out.flush();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Tests stream read from specified file.
     *
     * @param file File to write from
     * @param bufSize Buffer size.
     * @param progress Progress that will be incremented on each written chunk.
     */
    private static void benchmarkRead(FileSystem fs, Path file, int bufSize, @Nullable AtomicLong progress)
        throws Exception {

        try (FSDataInputStream in = fs.open(file, bufSize)) {
            byte[] data = new byte[32 * bufSize];

            while (true) {
                int read = in.read(data);

                if (read < 0)
                    return;

                if (progress != null)
                    progress.addAndGet(read);
            }
        }
    }
}
