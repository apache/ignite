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

package org.gridgain.grid.kernal.processors.email;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Email processor.
 */
public abstract class GridEmailProcessorAdapter extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    protected GridEmailProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Sends given email to all admin emails, if any, in the current thread blocking until it's either
     * successfully sent or failed. If SMTP is disabled or admin emails are not provided - this method is no-op.
     *
     * @param subj Email subject.
     * @param body Email body.
     * @param html HTML format flag.
     * @throws IgniteCheckedException Thrown in case of any failure on sending.
     */
    public abstract void sendNow(String subj, String body, boolean html) throws IgniteCheckedException;

    /**
     * Sends given email in the current thread blocking until it's either successfully sent or failed.
     * If SMTP is disabled - this method is no-op.
     *
     * @param subj Email subject.
     * @param body Email body.
     * @param html HTML format flag.
     * @param addrs Addresses.
     * @throws IgniteCheckedException Thrown in case of any failure on sending.
     */
    public abstract void sendNow(String subj, String body, boolean html, Collection<String> addrs) throws IgniteCheckedException;

    /**
     * Schedules sending of given email to all admin emails, if any. If SMTP is disabled or admin emails
     * are not provided - this method is no-op. Emails will be send asynchronously from a different thread.
     * If email sending fails - the error log will be created for each email.
     *
     * @param subj Email subject.
     * @param body Email body.
     * @param html HTML format flag.
     * @return Future for scheduled email.
     */
    public abstract IgniteFuture<Boolean> schedule(String subj, String body, boolean html);

    /**
     * Schedules sending of given email. If SMTP is disabled - this method is no-op. Emails will be send
     * asynchronously from a different thread. If email sending fails - the error log will be created
     * for each email.
     *
     * @param subj Email subject.
     * @param body Email body.
     * @param html HTML format flag.
     * @param addrs Addresses.
     * @return Future for scheduled email.
     */
    public abstract IgniteFuture<Boolean> schedule(String subj, String body, boolean html, Collection<String> addrs);
}
