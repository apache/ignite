/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.email;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;

/**
 * Email holder for email processor.
 *
 * @author @java.author
 * @version @java.version
 */
class GridEmailHolder {
    /** */
    private String subj;

    /** */
    private String body;

    /** */
    private boolean html;

    /** */
    private Collection<String> addrs;

    /** */
    private GridFutureAdapter<Boolean> fut;

    /**
     *
     * @param fut Associated future.
     * @param subj Email subject.
     * @param body Email body.
     * @param html Whether email body is html.
     * @param addrs Addresses to send email to.
     */
    GridEmailHolder(GridFutureAdapter<Boolean> fut, String subj, String body, boolean html, Collection<String> addrs) {
        assert fut != null;
        assert subj != null;
        assert body != null;
        assert addrs != null;
        assert !addrs.isEmpty();

        this.fut = fut;
        this.subj = subj;
        this.body = body;
        this.html = html;
        this.addrs = addrs;
    }


    /**
     *
     * @return Holder's future.
     */
    GridFutureAdapter<Boolean> future() {
        return fut;
    }

    /**
     *
     * @return Email subject.
     */
    String subject() {
        return subj;
    }

    /**
     *
     * @return Email body.
     */
    String body() {
        return body;
    }

    /**
     *
     * @return Html flag.
     */
    boolean html() {
        return html;
    }

    /**
     *
     * @return Destinations.
     */
    Collection<String> addresses() {
        return addrs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEmailHolder.class, this);
    }
}
