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
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.worker.*;

import java.util.*;

/**
 * Email (SMTP) processor. Responsible for sending emails.
 */
public class GridEmailProcessor extends GridProcessorAdapter {
    /** Maximum emails queue size. */
    public static final int QUEUE_SIZE = 1024;

    /** */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Deque<GridEmailHolder> q;

    /** */
    private GridThread snd;

    /** */
    private GridWorker worker;

    /** */
    private final boolean isSmtpEnabled;

    /**
     * @param ctx Kernal context.
     */
    public GridEmailProcessor(GridKernalContext ctx) {
        super(ctx);

        isSmtpEnabled = ctx.config().getSmtpHost() != null;

        if (isSmtpEnabled) {
            worker = new GridWorker(ctx.config().getGridName(), "email-sender-worker", log) {
                @SuppressWarnings({"SynchronizeOnNonFinalField"})
                @Override protected void body() throws InterruptedException {
                    while (!Thread.currentThread().isInterrupted())
                        synchronized (q) {
                            while (q.isEmpty())
                                q.wait();

                            GridEmailHolder email = q.removeFirst();

                            assert email != null;

                            try {
                                sendNow(email.subject(), email.body(), email.html(), email.addresses());

                                email.future().onDone(true);
                            }
                            catch (GridException e) {
                                U.error(log, "Failed to send email with subject: " + email.subject(), e);

                                email.future().onDone(e);
                            }
                        }
                }
            };
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (isSmtpEnabled) {
            assert q == null;
            assert snd == null;

            q = new LinkedList<>();

            snd = new GridThread(ctx.config().getGridName(), "email-sender-thread", worker);

            snd.start();
        }

        if (log.isDebugEnabled())
            log.debug("Started email processor" + (isSmtpEnabled ? "." : " (inactive)."));
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        if (isSmtpEnabled) {
            U.interrupt(snd);
            U.join(snd, log);

            snd = null;

            if (q != null) {
                if (!q.isEmpty())
                    U.warn(log, "Emails queue is not empty on email processor stop.");

                q.clear();

                q = null;
            }
        }

        if (log.isDebugEnabled())
            log.debug("Stopped email processor.");
    }

    /**
     * Sends given email to all admin emails, if any, in the current thread blocking until it's either
     * successfully sent or failed. If SMTP is disabled or admin emails are not provided - this method is no-op.
     *
     * @param subj Email subject.
     * @param body Email body.
     * @param html HTML format flag.
     * @throws GridException Thrown in case of any failure on sending.
     */
    public void sendNow(String subj, String body, boolean html) throws GridException {
        String[] addrs = ctx.config().getAdminEmails();

        if (addrs != null && addrs.length > 0)
            sendNow(subj, body, html, Arrays.asList(addrs));
    }

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
    public void sendNow(String subj, String body, boolean html, Collection<String> addrs) throws GridException {
        assert subj != null;
        assert body != null;
        assert addrs != null;
        assert !addrs.isEmpty();

        if (isSmtpEnabled) {
            GridConfiguration cfg = ctx.config();

            U.sendEmail(
                // Static SMTP configuration data.
                cfg.getSmtpHost(),
                cfg.getSmtpPort(),
                cfg.isSmtpSsl(),
                cfg.isSmtpStartTls(),
                cfg.getSmtpUsername(),
                cfg.getSmtpPassword(),
                cfg.getSmtpFromEmail(),

                // Per-email data.
                subj,
                body,
                html,
                addrs
            );
        }
    }

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
    public GridFuture<Boolean> schedule(String subj, String body, boolean html) {
        String[] addrs = ctx.config().getAdminEmails();

        return addrs == null || addrs.length == 0 ? new GridFinishedFuture<>(ctx, false) :
            schedule(subj, body, html, Arrays.asList(addrs));
    }

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
    @SuppressWarnings({"SynchronizeOnNonFinalField"})
    public GridFuture<Boolean> schedule(String subj, String body, boolean html, Collection<String> addrs) {
        assert subj != null;
        assert body != null;
        assert addrs != null;
        assert !addrs.isEmpty();

        if (isSmtpEnabled)
            synchronized (q) {
                if (q.size() == QUEUE_SIZE) {
                    U.warn(log, "Email '" + subj + "' failed to schedule b/c queue is full.");

                    return new GridFinishedFuture<>(ctx, false);
                }
                else {
                    GridFutureAdapter<Boolean> fut = new GridFutureAdapter<Boolean>(ctx) {
                        @SuppressWarnings({"SynchronizeOnNonFinalField"})
                        @Override public boolean cancel() {
                            synchronized (q) {
                                for (GridEmailHolder email : q)
                                    if (email.future() == this) {
                                        q.remove(email); // We accept full scan on removal here.

                                        return true;
                                    }
                            }

                            return false;
                        }
                    };

                    q.addLast(new GridEmailHolder(fut, subj, body, html, addrs));

                    q.notifyAll();

                    return fut;
                }
            }
        else
            return new GridFinishedFuture<>(ctx, false);
    }
}
