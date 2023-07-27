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

package org.apache.ignite.internal.management.dump;

import java.util.Arrays;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/** */
public class DumpCreateTask extends VisorOneNodeTask<DumpCreateCommandArg, Void> {
    /** {@inheritDoc} */
    @Override protected VisorJob<DumpCreateCommandArg, Void> job(DumpCreateCommandArg arg) {
        return new DumpCreateJob(arg);
    }

    /** */
    private static class DumpCreateJob extends VisorJob<DumpCreateCommandArg, Void> {
        /** */
        protected DumpCreateJob(@Nullable DumpCreateCommandArg arg) {
            super(arg, false);
        }

        /** {@inheritDoc} */
        @Override protected Void run(@Nullable DumpCreateCommandArg arg) throws IgniteException {
            ignite.snapshot().createDump(arg.name(), arg.groups() == null ? null : Arrays.asList(arg.groups())).get();

            return null;
        }
    }
}
