package org.apache.ignite.internal.processors.odbc;

import java.util.HashMap;
import java.util.Map;

/**
 * ODBC protocol version.
 */
public enum OdbcProtocolVersion {
    /** First version of the ODBC. Released with Ignite 1.6 */
    VERSION_1_6_0(1),

    /** Second version of the ODBC. Released with Ignite 2.0 */
    VERSION_2_0_0(0x0002000000000000L),

    /** Unknown version. */
    VERSION_UNKNOWN(Long.MIN_VALUE);

    /** Long value to enum map. */
    private static final Map<Long, OdbcProtocolVersion> versions = new HashMap<>();

    /** Enum value to Ignite version map */
    private static final Map<OdbcProtocolVersion, String> since = new HashMap<>();

    /**
     * Map long values to version.
     */
    static {
        for (OdbcProtocolVersion version : values())
            versions.put(version.longValue(), version);

        since.put(VERSION_1_6_0, "1.6.0");
        since.put(VERSION_2_0_0, "2.0.0");
    }

    /** Long value for version. */
    private final long longVal;

    /**
     * @param longVal Long value.
     */
    OdbcProtocolVersion(long longVal) {
        this.longVal = longVal;
    }

    /**
     * @param longVal Long value.
     * @return Protocol version.
     */
    public static OdbcProtocolVersion fromLong(long longVal) {
        OdbcProtocolVersion res = versions.get(longVal);

        return res == null ? VERSION_UNKNOWN : res;
    }

    /**
     * @return Current version.
     */
    public static OdbcProtocolVersion current() {
        return VERSION_2_0_0;
    }

    /**
     * @return Long value.
     */
    public long longValue() {
        return longVal;
    }

    /**
     * @return {@code true} if this version is unknown.
     */
    public boolean isUnknown() {
        return longVal == VERSION_UNKNOWN.longVal;
    }

    /**
     * @return {@code true} if this version supports distributed joins.
     */
    public boolean isDistributedJoinsSupported() {
        assert !isUnknown();

        return longVal >= VERSION_2_0_0.longVal;
    }

    /**
     * @return Ignite version when introduced.
     */
    public String since() {
        assert !isUnknown();

        return since.get(this);
    }
}
