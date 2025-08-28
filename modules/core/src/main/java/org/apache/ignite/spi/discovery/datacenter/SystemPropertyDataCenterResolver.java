package org.apache.ignite.spi.discovery.datacenter;

import org.apache.ignite.IgniteSystemProperties;

import static org.apache.ignite.IgniteCommonsSystemProperties.getString;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_CENTER_ID;

/**
 * Implementation of {@link DataCenterResolver} that uses {@link IgniteSystemProperties#IGNITE_DATA_CENTER_ID} system property
 * to resolve data center ID.
 */
public class SystemPropertyDataCenterResolver implements DataCenterResolver {
    /** {@inheritDoc} */
    @Override public String resolveDataCenterId() {
        String dcId = getString(IGNITE_DATA_CENTER_ID);

        return dcId;
    }
}
