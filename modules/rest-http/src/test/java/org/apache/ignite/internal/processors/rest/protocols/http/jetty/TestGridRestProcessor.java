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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.util.function.BiConsumer;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;

import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.TestRestProcessorPluginProvider.PLUGIN_NAME;

/**
 *
 */
public class TestGridRestProcessor extends GridRestProcessor {
    /** Request / response listener. */
    private BiConsumer<GridRestRequest, IgniteInternalFuture<GridRestResponse>> lsnr;

    /**
     * @param ctx Context.
     */
    public TestGridRestProcessor(GridKernalContext ctx) {
        super(ctx);

        TestRestProcessorPluginConfiguration cfg = ((TestRestProcessorPluginProvider) ctx.pluginProvider(PLUGIN_NAME))
            .getConfiguration();

        lsnr = cfg.getListener();
    }

    /** */
    public BiConsumer<GridRestRequest, IgniteInternalFuture<GridRestResponse>> getListener() {
        return lsnr;
    }

    /** */
    public void setListener(BiConsumer<GridRestRequest, IgniteInternalFuture<GridRestResponse>> lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<GridRestResponse> handleAsync0(GridRestRequest req) {
        IgniteInternalFuture<GridRestResponse> fut = super.handleAsync0(req);

        if (lsnr != null)
            fut.listen(f -> lsnr.accept(req, f));

        return fut;
    }
}
