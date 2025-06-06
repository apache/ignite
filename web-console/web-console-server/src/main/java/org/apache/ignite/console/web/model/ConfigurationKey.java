

package org.apache.ignite.console.web.model;

import java.util.Objects;
import java.util.UUID;

/**
 * Key for configurations.
 */
public class ConfigurationKey {
    /** Account ID. */
    private UUID accId;
    
    /** Demo. */
    private boolean demo;

    /**
     * @param accId Account ID.
     * @param demo Demo.
     */
    public ConfigurationKey(UUID accId, boolean demo) {
        this.accId = accId;
        this.demo = demo;
    }

    /**
     * @return value of account ID.
     */
    public UUID getAccId() {
        return accId;
    }

    /**
     * @return value of demo
     */
    public boolean isDemo() {
        return demo;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ConfigurationKey key = (ConfigurationKey)o;
        
        return demo == key.demo && Objects.equals(accId, key.accId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(accId, demo);
    }
}
