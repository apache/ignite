package org.apache.ignite.internal.management.api;

import java.util.Collection;
import java.util.List;

/** @inheritDoc */
public class ExternalTestCommandsProvider implements CommandsProvider {
    /** @inheritDoc */
    @Override public Collection<Command<?, ?>> commands() {
        return List.of(new ExternalTestCommand());
    }

    /** */
    static class ExternalTestCommand implements Command<NoArg, Void> {
        /** @inheritDoc */
        @Override public String description() {
            return "Test Command";
        }

        /** @inheritDoc */
        @Override public Class<NoArg> argClass() {
            return NoArg.class;
        }
    }
}
