/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbcx;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import javax.transaction.xa.Xid;
import org.h2.util.MathUtils;

/**
 * A simple Xid implementation.
 */
public class SimpleXid implements Xid {

    private static AtomicInteger next = new AtomicInteger();

    private final int formatId;
    private final byte[] branchQualifier;
    private final byte[] globalTransactionId;

    private SimpleXid(int formatId, byte[] branchQualifier,
            byte[] globalTransactionId) {
        this.formatId = formatId;
        this.branchQualifier = branchQualifier;
        this.globalTransactionId = globalTransactionId;
    }

    /**
     * Create a new random xid.
     *
     * @return the new object
     */
    public static SimpleXid createRandom() {
        int formatId = next.getAndIncrement();
        byte[] bq = new byte[MAXBQUALSIZE];
        MathUtils.randomBytes(bq);
        byte[] gt = new byte[MAXGTRIDSIZE];
        MathUtils.randomBytes(gt);
        return new SimpleXid(formatId, bq, gt);
    }

    @Override
    public byte[] getBranchQualifier() {
        return branchQualifier;
    }

    @Override
    public int getFormatId() {
        return formatId;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return globalTransactionId;
    }

    @Override
    public int hashCode() {
        return formatId;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Xid) {
            Xid xid = (Xid) other;
            if (xid.getFormatId() == formatId) {
                if (Arrays.equals(branchQualifier, xid.getBranchQualifier())) {
                    if (Arrays.equals(globalTransactionId, xid.getGlobalTransactionId())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "xid:" + formatId;
    }

}
