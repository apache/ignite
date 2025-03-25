package org.apache.ignite.internal.commandline;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.JmxCommandsProvider;
import org.apache.ignite.internal.util.typedef.internal.U;

/** @inheritDoc */
public class JmxCommandsProviderImpl implements JmxCommandsProvider {
    /** @inheritDoc */
    @Override public Collection<Command<?, ?>> commands() {
        Iterable<CommandsProvider> providers = U.loadService(CommandsProvider.class);

        return StreamSupport.stream(providers.spliterator(),
            false).flatMap(provider -> provider.commands().stream()).collect(Collectors.toList());
    }
}
