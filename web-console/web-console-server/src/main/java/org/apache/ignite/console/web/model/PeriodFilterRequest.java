

package org.apache.ignite.console.web.model;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Web model for period filter request.
 */
public class PeriodFilterRequest {
    /** */
    private long startDate;

    /** */
    private long endDate;

    /**
     * @return Start date.
     */
    public long getStartDate() {
        return startDate;
    }

    /**
     * @param startDate Start date.
     */
    public void setStartDate(long startDate) {
        this.startDate = startDate;
    }

    /**
     * @return End date.
     */
    public long getEndDate() {
        return endDate;
    }

    /**
     * @param endDate End date.
     */
    public void setEndDate(long endDate) {
        this.endDate = endDate;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PeriodFilterRequest.class, this);
    }
}
