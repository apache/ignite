package org.apache.ignite.internal.management.api;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** @inheritDoc */
public class TestCommandsProvider implements CommandsProvider {
    /** @inheritDoc */
    @Override public Collection<Command<?, ?>> commands() {
        return List.of(new TestCommand());
    }

    /** */
    static class TestCommand implements Command {
        /** @inheritDoc */
        @Override public String description() {
            return "Test Command";
        }

        /** @inheritDoc */
        @Override public Class<? extends IgniteDataTransferObject> argClass() {
            return null;
        }
    }
}
