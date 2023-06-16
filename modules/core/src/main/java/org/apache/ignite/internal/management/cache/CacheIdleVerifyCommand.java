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

package org.apache.ignite.internal.management.cache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.api.CliSubcommandsWithPrefix;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLoggerFileHandler;
import static org.apache.ignite.internal.management.cache.VerifyBackupPartitionsDumpTask.logParsedArgs;

/** Checks consistency of primary and backup partitions assuming no concurrent updates are happening in the cluster. */
@CliSubcommandsWithPrefix
public class CacheIdleVerifyCommand
    extends CommandRegistryImpl<CacheIdleVerifyCommandArg, IdleVerifyTaskResultV2>
    implements ComputeCommand<CacheIdleVerifyCommandArg, IdleVerifyTaskResultV2> {
    /** */
    public static final String IDLE_VERIFY_FILE_PREFIX = "idle_verify-";

    /** Time formatter for log file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss_SSS");

    /** */
    public CacheIdleVerifyCommand() {
        super(new CacheIdleVerifyDumpCommand());
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Verify counters and hash sums of primary and backup partitions for the specified " +
            "caches/cache groups on an idle cluster and print out the differences, if any. " +
            "Cache filtering options configure the set of caches that will be processed by idle_verify command. " +
            "Default value for the set of cache names (or cache group names) is all cache groups. Default value" +
            " for --exclude-caches is empty set. " +
            "Default value for --cache-filter is no filtering. Therefore, the set of all caches is sequently " +
            "filtered by cache name " +
            "regexps, by cache type and after all by exclude regexps";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheIdleVerifyCommandArg> argClass() {
        return CacheIdleVerifyCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<IdleVerifyTaskV2> taskClass() {
        return IdleVerifyTaskV2.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(CacheIdleVerifyCommandArg arg, IdleVerifyTaskResultV2 res, Consumer<String> printer) {
        logParsedArgs(arg, printer);

        StringBuilder sb = new StringBuilder();
        res.print(sb::append, false);
        printer.accept(sb.toString());

        if (F.isEmpty(res.exceptions()) && !res.hasConflicts())
            return;

        try {
            File f = new File(JavaLoggerFileHandler.logDirectory(U.defaultWorkDirectory()),
                IDLE_VERIFY_FILE_PREFIX + LocalDateTime.now().format(TIME_FORMATTER) + ".txt");

            try (PrintWriter pw = new PrintWriter(f)) {
                res.print(pw::print, true);
                pw.flush();

                printer.accept("See log for additional information. " + f.getAbsolutePath());
            }
            catch (FileNotFoundException e) {
                printer.accept("Can't write exceptions to file " + f.getAbsolutePath());

                throw new IgniteException(e);
            }
        }
        catch (IgniteCheckedException e) {
            printer.accept("Can't find work directory. " + e.getMessage());

            throw new IgniteException(e);
        }
    }
}
