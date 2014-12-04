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
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.worker.*;

import javax.mail.*;
import javax.mail.internet.*;
import java.util.*;

/**
 * Email (SMTP) processor. Responsible for sending emails.
 */
public class GridEmailProcessor extends GridEmailProcessorAdapter {
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

    /** {@inheritDoc} */
    @Override public void sendNow(String subj, String body, boolean html) throws GridException {
        String[] addrs = ctx.config().getAdminEmails();

        if (addrs != null && addrs.length > 0)
            sendNow(subj, body, html, Arrays.asList(addrs));
    }

    /** {@inheritDoc} */
    @Override public void sendNow(String subj, String body, boolean html, Collection<String> addrs)
        throws GridException {
        assert subj != null;
        assert body != null;
        assert addrs != null;
        assert !addrs.isEmpty();

        if (isSmtpEnabled) {
            IgniteConfiguration cfg = ctx.config();

            sendEmail(
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

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> schedule(String subj, String body, boolean html) {
        String[] addrs = ctx.config().getAdminEmails();

        return addrs == null || addrs.length == 0 ? new GridFinishedFuture<>(ctx, false) :
            schedule(subj, body, html, Arrays.asList(addrs));
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SynchronizeOnNonFinalField"})
    @Override public GridFuture<Boolean> schedule(String subj, String body, boolean html, Collection<String> addrs) {
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
    /**
     *
     * @param smtpHost SMTP host.
     * @param smtpPort SMTP port.
     * @param ssl SMTP SSL.
     * @param startTls Start TLS flag.
     * @param username Email authentication user name.
     * @param pwd Email authentication password.
     * @param from From email.
     * @param subj Email subject.
     * @param body Email body.
     * @param html HTML format flag.
     * @param addrs Addresses to send email to.
     * @throws GridException Thrown in case when sending email failed.
     */
    public static void sendEmail(String smtpHost, int smtpPort, boolean ssl, boolean startTls, final String username,
        final String pwd, String from, String subj, String body, boolean html, Collection<String> addrs)
        throws GridException {
        assert smtpHost != null;
        assert smtpPort > 0;
        assert from != null;
        assert subj != null;
        assert body != null;
        assert addrs != null;
        assert !addrs.isEmpty();

        Properties props = new Properties();

        props.setProperty("mail.transport.protocol", "smtp");
        props.setProperty("mail.smtp.host", smtpHost);
        props.setProperty("mail.smtp.port", Integer.toString(smtpPort));

        if (ssl)
            props.setProperty("mail.smtp.ssl", "true");

        if (startTls)
            props.setProperty("mail.smtp.starttls.enable", "true");

        Authenticator auth = null;

        // Add property for authentication by username.
        if (username != null && !username.isEmpty()) {
            props.setProperty("mail.smtp.auth", "true");

            auth = new Authenticator() {
                @Override public PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, pwd);
                }
            };
        }

        Session ses = Session.getInstance(props, auth);

        MimeMessage email = new MimeMessage(ses);

        try {
            email.setFrom(new InternetAddress(from));
            email.setSubject(subj);
            email.setSentDate(new Date());

            if (html)
                email.setText(body, "UTF-8", "html");
            else
                email.setText(body);

            Address[] rcpts = new Address[addrs.size()];

            int i = 0;

            for (String addr : addrs)
                rcpts[i++] = new InternetAddress(addr);

            email.setRecipients(MimeMessage.RecipientType.TO, rcpts);

            Transport.send(email);
        }
        catch (MessagingException e) {
            throw new GridException("Failed to send email.", e);
        }
    }
}
