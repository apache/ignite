/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision;

import java.util.*;

/**
 * Listener to be set on {@link CollisionSpi} for notification of external
 * collision events (e.g. job stealing). Once grid receives such notification,
 * it will immediately invoke collision SPI.
 */
public interface CollisionExternalListener extends EventListener {
    /**
     * Callback invoked by Collision SPI whenever an external collision
     * event occurs.
     */
    public void onExternalCollision();
}
