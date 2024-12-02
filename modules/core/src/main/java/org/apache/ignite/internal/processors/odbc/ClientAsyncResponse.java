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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Client async response.
 */
public class ClientAsyncResponse extends ClientResponse implements ClientListenerAsyncResponse {
    /** Future for response. */
    private final IgniteInternalFuture<? extends ClientListenerResponse> fut;

    /**
     * Constructs async response.
     */
    public ClientAsyncResponse(long reqId, IgniteInternalFuture<? extends ClientListenerResponse> fut) {
        super(reqId);

        this.fut = fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<? extends ClientListenerResponse> future() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public int status() {
        assert fut.isDone();

        try {
            return fut.get().status();
        }
        catch (Exception e) {
            return STATUS_FAILED;
        }
    }

    /** {@inheritDoc} */
    @Override protected void status(int status) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public String error() {
        assert fut.isDone();

        try {
            return fut.get().error();
        }
        catch (Exception e) {
            return e.getMessage();
        }
    }

    /** {@inheritDoc} */
    @Override protected void error(String err) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void onSent() {
        assert fut.isDone();

        try {
            fut.get().onSent();
        }
        catch (Exception ignore) {
            // Ignore.
        }
    }
}
