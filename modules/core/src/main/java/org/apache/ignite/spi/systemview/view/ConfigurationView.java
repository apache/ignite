package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;

/**
 * Configuration value representation for a {@link SystemView}.
 */
public class ConfigurationView {
    /** Name of the configuration property. */
    private final String name;

    /** Value of the configuration property. */
    private final String val;

    /**
     * @param name Name of the configuration property.
     * @param val Value of the configuration property.
     */
    public ConfigurationView(String name, String val) {
        this.name = name;
        this.val = val;
    }

    /** @return Name of the configuration property. */
    @Order
    public String name() {
        return name;
    }

    /** @return Value of the configuration property. */
    public String value() {
        return val;
    }
}
