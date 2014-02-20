// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid;

import org.gridgain.examples.datagrid.query.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.product.*;

import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Person record used for affinity query examples, e.g. {@link GridCacheQueryExample}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class AffinityPerson extends Person {
    /** Custom cache key to guarantee that person is always collocated with its organization. */
    private transient GridCacheAffinityKey<UUID> key;

    /**
     * Constructs person record.
     *
     * @param org Organization.
     * @param firstName First name.
     * @param lastName Last name.
     * @param salary Salary.
     * @param resume Resume text.
     */
    public AffinityPerson(Organization org, String firstName, String lastName, double salary, String resume) {
        super(org, firstName, lastName, salary, resume);
    }

    /**
     * Gets cache affinity key. Since in some examples person needs to be collocated with organization, we create
     * custom affinity key to guarantee this collocation.
     *
     * @return Custom affinity key to guarantee that person is always collocated with organization.
     */
    public GridCacheAffinityKey<UUID> key() {
        if (key == null)
            key = new GridCacheAffinityKey<>(getId(), getOrganizationId());

        return key;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("AffinityPerson ");
        sb.append("[key=").append(key);
        sb.append(", super=").append(super.toString());
        sb.append(']');

        return sb.toString();
    }
}
