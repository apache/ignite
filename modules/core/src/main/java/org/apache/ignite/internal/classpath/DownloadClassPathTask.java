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

package org.apache.ignite.internal.classpath;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 *
 */
public class DownloadClassPathTask implements Callable<IgniteInternalFuture<ClassPathDeployToAllResponse>> {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Ignite class path. */
    private final IgniteClassPath icp;

    /** Resulting future. */
    private final GridFutureAdapter<ClassPathDeployToAllResponse> res;

    /**
     * @param ctx Kernal context.
     * @param icp Ignite class path.
     */
    public DownloadClassPathTask(GridKernalContext ctx, IgniteClassPath icp) {
        this.ctx = ctx;
        this.log = ctx.log(DownloadClassPathTask.class);
        this.icp = icp;
        this.res = new GridFutureAdapter<>();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<ClassPathDeployToAllResponse> call() {
        Stream.of(icp.files())
            .map(DownloadClassPathFileTask::new)
            .map(ctx.pools().getPeerClassLoadingExecutorService()::submit)
            .forEach(fileFut -> {
                if (res.isDone())
                    fileFut.cancel(true);

                try {
                    fileFut.get();
                }
                catch (ExecutionException | InterruptedException e) {
                    if (!res.isDone()) {
                        log.warning("Classpath download files error: " + icp, e);

                        res.onDone(e);
                    }
                }
            });

        res.onDone(new ClassPathDeployToAllResponse(icp.id()));

        return res;
    }

    /** */
    private class DownloadClassPathFileTask implements Runnable {
        /** */
        private final String name;

        /** */
        public DownloadClassPathFileTask(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            log.info("Downloading file: " + name);
        }
    }
}
