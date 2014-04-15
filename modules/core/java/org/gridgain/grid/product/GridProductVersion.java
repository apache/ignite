/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.product;

import org.apache.commons.codec.*;
import org.apache.commons.codec.binary.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

/**
 * Represents node version.
 * <p>
 * Node version can be acquired via {@link GridNode#version()} method.
 * <p>
 * Two versions are compared in the following order: major number,
 * minor number, maintenance number, revision timestamp.
 */
public class GridProductVersion implements Comparable<GridProductVersion>, Externalizable {
    /** Development version. This version is larger than any other version. */
    public static final GridProductVersion VERSION_DEV =
        new GridProductVersion(Byte.MAX_VALUE, (byte)0, (byte)0, 0, null);

    /** Unknown version. This version is less than any other version. */
    public static final GridProductVersion VERSION_UNKNOWN =
        new GridProductVersion((byte)-1, (byte)0, (byte)0, 0, null);

    /** Regexp parse pattern. */
    private static final Pattern VER_PATTERN =
        Pattern.compile("(.+-)?(\\d+)\\.(\\d+)\\.(\\d+)(-(\\d+))?(-([0-9a-f]+))?");

    /** Development version string. */
    private static final String DEV_VERSION_STR = "x.x.x-0-DEV";
    private static final long serialVersionUID = 0L;


    /** Major version number. */
    private byte major;

    /** Minor version number. */
    private byte minor;

    /** Maintenance version number. */
    private byte maintenance;

    /** Revision timestamp. */
    private long revTs;

    /** Revision hash. */
    private byte[] revHash;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridProductVersion() {
        // No-op.
    }

    /**
     * @param major Major version number.
     * @param minor Minor version number.
     * @param maintenance Maintenance version number.
     * @param revTs Revision timestamp.
     * @param revHash Revision hash.
     */
    public GridProductVersion(byte major, byte minor, byte maintenance, long revTs, byte[] revHash) {
        if (revHash != null && revHash.length != 20)
            throw new IllegalArgumentException("Invalid length for SHA1 hash (must be 20): " + revHash.length);

        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
        this.revTs = revTs;
        this.revHash = revHash != null ? revHash : new byte[20];
    }

    /**
     * Gets major version number.
     *
     * @return Major version number.
     */
    public byte major() {
        return major;
    }

    /**
     * Gets minor version number.
     *
     * @return Minor version number.
     */
    public byte minor() {
        return minor;
    }

    /**
     * Gets maintenance version number.
     *
     * @return Maintenance version number.
     */
    public byte maintenance() {
        return maintenance;
    }

    /**
     * Gets revision timestamp.
     *
     * @return Revision timestamp.
     */
    public long revisionTimestamp() {
        return revTs;
    }

    /**
     * Gets revision hash.
     *
     * @return Revision hash.
     */
    public byte[] revisionHash() {
        return revHash;
    }

    /**
     * Gets release date.
     *
     * @return Release date.
     */
    public Date releaseDate() {
        return new Date(revTs);
    }


    /** {@inheritDoc} */
    @Override public int compareTo(GridProductVersion o) {
        // NOTE:
        // Unknown version is less than any other version.
        // Developer's version is greater than any other version.
        if (major == o.major) {
            if (minor == o.minor) {
                if (maintenance == o.maintenance)
                    return revTs != o.revTs ? revTs < o.revTs ? -1 : 1 : 0;
                else
                    return maintenance < o.maintenance ? -1 : 1;
            }
            else
                return minor < o.minor ? -1 : 1;
        }
        else
            return major < o.major ? -1 : 1;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridProductVersion))
            return false;

        GridProductVersion that = (GridProductVersion)o;

        return revTs == that.revTs && maintenance == that.maintenance && minor == that.minor && major == that.major;

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = major;

        res = 31 * res + minor;
        res = 31 * res + maintenance;
        res = 31 * res + (int)(revTs ^ (revTs >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(major);
        out.writeByte(minor);
        out.writeByte(maintenance);
        out.writeLong(revTs);
        U.writeByteArray(out, revHash);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        major = in.readByte();
        minor = in.readByte();
        maintenance = in.readByte();
        revTs = in.readLong();
        revHash = U.readByteArray(in);
    }

    /**
     * @return Resolved object.
     * @throws ObjectStreamException In case of error.
     */
    protected Object readResolve() throws ObjectStreamException {
        if (major == Integer.MAX_VALUE)
            return VERSION_DEV;

        if (major == -1)
            return VERSION_UNKNOWN;

        return this;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridProductVersion.class, this);
    }

    /**
     * Tries to parse product version from it's string representation. Will return {@link #VERSION_UNKNOWN}
     * if string does not conform to version format.
     *
     * @param verStr String representation of version.
     * @return Product version.
     */
    @SuppressWarnings({"MagicConstant", "TypeMayBeWeakened"})
    public static GridProductVersion fromString(@Nullable String verStr) {
        if (verStr == null)
            return VERSION_UNKNOWN;

        if (DEV_VERSION_STR.equals(verStr))
            return VERSION_DEV;

        Matcher match = VER_PATTERN.matcher(verStr);

        if (match.matches()) {
            try {
                byte major = Byte.parseByte(match.group(2));
                byte minor = Byte.parseByte(match.group(3));
                byte maintenance = Byte.parseByte(match.group(4));

                long revTs = 0;

                if (match.group(6) != null)
                    revTs = Long.parseLong(match.group(6));

                byte[] revHash = null;

                if (match.group(8) != null)
                    revHash = Hex.decodeHex(match.group(8).toCharArray());

                return new GridProductVersion(major, minor, maintenance, revTs, revHash);
            }
            catch (IllegalStateException | IndexOutOfBoundsException ignored) {
                return VERSION_UNKNOWN;
            }
            catch (NumberFormatException | DecoderException ignored) {
                // Safety, should never happen.
                return VERSION_UNKNOWN;
            }
        }
        else
            return VERSION_UNKNOWN;
    }
}
