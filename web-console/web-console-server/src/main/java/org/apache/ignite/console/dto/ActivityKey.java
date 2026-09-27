

package org.apache.ignite.console.dto;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Activity info.
 */
public class ActivityKey {
    /** */
    private UUID owner;

    /** */
    private long date;

    /**
     * Default constructor for serialization.
     */
    public ActivityKey() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param owner Owner ID.
     * @param date Activity period (year and month).
     */
    public ActivityKey(UUID owner, long date) {
        this.owner = owner;
        this.date = date;
    }

    /**
     * @return Owner.
     */
    public UUID getOwner() {
        return owner;
    }

    /**
     * @param owner Owner.
     */
    public void setOwner(UUID owner) {
        this.owner = owner;
    }

    /**
     * @return Activity period.
     */
    public long getDate() {
        return date;
    }

    /**
     * @param date Activity period.
     */
    public void setDate(long date) {
        this.date = date;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        ActivityKey key = (ActivityKey)obj;

        return date == key.date && Objects.equals(owner, key.owner);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(owner, date);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ActivityKey.class, this);
    }
}
