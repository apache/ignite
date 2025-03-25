package org.apache.ignite.internal.management.api;

import java.util.Collection;

/** External commands provider plugin to register in JMX */
public interface JmxCommandsProvider {
    /** Gets all supported by this provider commands. */
    Collection<Command<?, ?>> commands();
}
