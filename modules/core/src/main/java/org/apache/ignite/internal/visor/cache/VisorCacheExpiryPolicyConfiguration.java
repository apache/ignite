package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Data transfer object for cache expiry policy configuration properties.
 */
public class VisorCacheExpiryPolicyConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** After create expire policy. */
    private String create;

    /** After access expire policy. */
    private String access;

    /** After update expire policy. */
    private String update;

    /**
     * Default constructor.
     */
    public VisorCacheExpiryPolicyConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for cache expiry policy configuration.
     *
     * @param ccfg Cache configuration.
     */
    public VisorCacheExpiryPolicyConfiguration(CacheConfiguration ccfg) {
        final Factory<ExpiryPolicy> expiryPolicyFactory = ccfg.getExpiryPolicyFactory();
        ExpiryPolicy expiryPolicy = expiryPolicyFactory.create();

        create = durationToString(expiryPolicy.getExpiryForCreation());
        access = durationToString(expiryPolicy.getExpiryForAccess());
        update = durationToString(expiryPolicy.getExpiryForUpdate());
    }

    /**
     * @return After create expire policy.
     */
    public String getCreate() {
        return create;
    }

    /**
     * @return After access expire policy.
     */
    public String getAccess() {
        return access;
    }

    /**
     * @return After update expire policy.
     */
    public String getUpdate() {
        return update;
    }

    /** {@inheritDoc} */
    @Override
    protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, create);
        U.writeString(out, access);
        U.writeString(out, update);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        create = U.readString(in);
        access = U.readString(in);
        update = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheExpiryPolicyConfiguration.class, this);
    }

    private String durationToString(Duration duration) {
        return duration != null ? String.valueOf(duration.getDurationAmount()) + " " + duration.getTimeUnit() : null;
    }
}
