package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Task argument for {@link VisorCacheMetricsToggleTask}.
 */
public class VisorCacheMetricsToggleTaskArg extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Metrics mode for an affected caches. */
    private boolean enableMetrics;

    /** Flag indicates if the operation should be performed on all user caches or on a specified list. */
    private boolean applyToAllCaches;

    /** Names of a caches which will be affected by task when <tt>applyToAllCaches</tt> is <code>false</code>. */
    private Set<String> cacheNames;

    /**
     * Default constructor.
     */
    public VisorCacheMetricsToggleTaskArg() {
        // No-op.
    }

    /**
     * @param enableMetrics Metrics mode.
     * @param cacheNames Affected cache names.
     */
    public VisorCacheMetricsToggleTaskArg(boolean enableMetrics, Set<String> cacheNames) {
        this.enableMetrics = enableMetrics;
        this.cacheNames = Collections.unmodifiableSet(cacheNames);

        applyToAllCaches = false;
    }

    /**
     * @param enableMetrics Metrics mode.
     * @param applyToAllCaches Apply to all caches flag.
     */
    public VisorCacheMetricsToggleTaskArg(boolean enableMetrics, boolean applyToAllCaches) {
        this.enableMetrics = enableMetrics;
        this.applyToAllCaches = applyToAllCaches;

        cacheNames = Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enableMetrics);
        out.writeBoolean(applyToAllCaches);
        U.writeCollection(out, cacheNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException,
        ClassNotFoundException {
        enableMetrics = in.readBoolean();
        applyToAllCaches = in.readBoolean();
        cacheNames = U.readSet(in);
    }

    /**
     * @return Metrics mode for an affected caches.
     */
    public boolean enableMetrics() {
        return enableMetrics;
    }

    /**
     * @return Flag indicates if the operation should be performed on all user caches or on a specified list.
     */
    public boolean applyToAllCaches() {
        return applyToAllCaches;
    }

    /**
     * @return Names of a caches which will be affected by task when <tt>applyToAllCaches</tt> is <code>false</code>.
     */
    public Collection<String> cacheNames() {
        return Collections.unmodifiableSet(cacheNames);
    }
}
