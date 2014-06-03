/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.event;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link GridLicenseEvent}.
 */
public class VisorGridLicenseEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** License ID. */
    private final UUID licenseId;

    /** Create event with given parameters. */
    public VisorGridLicenseEvent(
        int typeId,
        GridUuid id,
        String name,
        UUID nid,
        long timestamp,
        String message,
        String shortDisplay,
        UUID licenseId
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.licenseId = licenseId;
    }

    /**
     * @return License ID.
     */
    public UUID licenseId() {
        return licenseId;
    }
}
