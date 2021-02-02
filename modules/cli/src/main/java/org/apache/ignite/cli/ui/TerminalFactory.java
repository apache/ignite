package org.apache.ignite.cli.ui;

import java.io.IOException;
import javax.inject.Singleton;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * Factory for producing JLine {@link Terminal} instances
 */
@Factory
public class TerminalFactory {
    /**
     * Produce terminal instances.
     *
     * Important: It's always must be a singleton bean.
     * JLine has an issues with building more than 1 terminal instance per process
     * @return Terminal instance
     */
    @Bean(preDestroy = "close")
    @Singleton
    public Terminal terminal() throws IOException {
        return TerminalBuilder.terminal();
    }
}
