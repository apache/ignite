package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Task argument for {@link VisorCacheMetricsStatusTask}.
 */
public class VisorCacheMetricsStatusTaskArg extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Flag indicates if the operation should be performed on all user caches or on a specified list. */
    private boolean applyToAllCaches;

    /** Names of a caches which will be affected by task when <tt>applyToAllCaches</tt> is <code>false</code>. */
    private Set<String> cacheNames;

    /**
     * Default constructor.
     */
    public VisorCacheMetricsStatusTaskArg() {
        // No-op.
    }

    /**
     * @param cacheNames Affected cache names.
     */
    public VisorCacheMetricsStatusTaskArg(Set<String> cacheNames) {
        this.cacheNames = Collections.unmodifiableSet(cacheNames);

        applyToAllCaches = false;
    }

    /**
     * @param applyToAllCaches Apply to all caches flag.
     */
    public VisorCacheMetricsStatusTaskArg(boolean applyToAllCaches) {
        this.applyToAllCaches = applyToAllCaches;

        cacheNames = Collections.emptySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(applyToAllCaches);
        U.writeCollection(out, cacheNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException,
        ClassNotFoundException {
        applyToAllCaches = in.readBoolean();
        cacheNames = U.readSet(in);
    }

    /**
     * @return Flag indicates if the operation should be performed on all user caches or on a specified list.
     */
    public boolean applyToAllCaches() {
        return applyToAllCaches;
    }

    /**
     * @return Names of a caches which will be affected by task when <tt>allCaches</tt> is <code>false</code>.
     */
    public Set<String> cacheNames() {
        return Collections.unmodifiableSet(cacheNames);
    }
}
