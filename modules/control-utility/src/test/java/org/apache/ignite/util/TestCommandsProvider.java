package org.apache.ignite.util;

import java.util.Collection;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsProvider;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class TestCommandsProvider implements CommandsProvider {
    /** {@inheritDoc} */
    @Override public Collection<Command<?, ?>> commands() {
        return F.asList(
            new GridCommandHandlerTest.OfflineTestCommand()
        );
    }
}
