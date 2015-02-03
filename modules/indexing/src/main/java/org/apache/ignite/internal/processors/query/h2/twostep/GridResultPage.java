/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Page result.
 */
public class GridResultPage<Z> {
    /** */
    private final Z src;

    /** */
    private final GridNextPageResponse res;

    /**
     * @param src Source.
     * @param res Response.
     */
    protected GridResultPage(Z src, GridNextPageResponse res) {
        this.src = src;
        this.res = res;
    }

    /**
     * @return Result source.
     */
    public Z source() {
        return src;
    }

    /**
     * @return Response.
     */
    public GridNextPageResponse response() {
        return res;
    }

    /**
     * Request next page.
     */
    public void fetchNextPage() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResultPage.class, this);
    }
}
