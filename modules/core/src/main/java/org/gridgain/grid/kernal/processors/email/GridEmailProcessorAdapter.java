/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.email;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;

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
     * @throws org.gridgain.grid.GridException Thrown in case of any failure on sending.
     */
    public abstract void sendNow(String subj, String body, boolean html) throws GridException;

    /**
     * Sends given email in the current thread blocking until it's either successfully sent or failed.
     * If SMTP is disabled - this method is no-op.
     *
     * @param subj Email subject.
     * @param body Email body.
     * @param html HTML format flag.
     * @param addrs Addresses.
     * @throws GridException Thrown in case of any failure on sending.
     */
    public abstract void sendNow(String subj, String body, boolean html, Collection<String> addrs) throws GridException;

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
    public abstract GridFuture<Boolean> schedule(String subj, String body, boolean html);

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
    public abstract GridFuture<Boolean> schedule(String subj, String body, boolean html, Collection<String> addrs);
}
