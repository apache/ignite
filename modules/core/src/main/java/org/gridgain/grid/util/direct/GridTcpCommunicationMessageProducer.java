/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

/**
 * Message producer. Each component have to register it's own message producer.
 */
public interface GridTcpCommunicationMessageProducer {
    /**
     * Create message.
     *
     * @param type Message type.
     * @return Communication message.
     */
    public GridTcpCommunicationMessageAdapter create(byte type);
}
