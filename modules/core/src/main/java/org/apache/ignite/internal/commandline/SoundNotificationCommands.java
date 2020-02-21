package org.apache.ignite.internal.commandline;

import org.apache.ignite.internal.client.GridClientConfiguration;

import java.awt.Toolkit;

import java.util.logging.Logger;

import static org.apache.ignite.internal.commandline.CommandList.BEEP_SOUND;

public class SoundNotificationCommands implements Command<Void> {

    /** {@inheritDoc} */
    @Override
    public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        Toolkit.getDefaultToolkit().beep();
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void printUsage(Logger logger) {
        Command.usage(logger, "Play a 'beep' sound after completion:", BEEP_SOUND);
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return BEEP_SOUND.name();
    }
}
