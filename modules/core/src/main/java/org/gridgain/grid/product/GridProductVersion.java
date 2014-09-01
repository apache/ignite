/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.product;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.text.*;
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
    /** */
    private static final long serialVersionUID = 0L;

    /** Regexp parse pattern. */
    private static final Pattern VER_PATTERN =
        Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)((?!-(os|ent))-([^-]+))?(-(os|ent))?(-(\\d+))?(-([\\da-f]+))?");

    /** Major version number. */
    private byte major;

    /** Minor version number. */
    private byte minor;

    /** Maintenance version number. */
    private byte maintenance;

    /** Stage of development. */
    private String stage;

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
        this(major, minor, maintenance, "", revTs, revHash);
    }

    /**
     * @param major Major version number.
     * @param minor Minor version number.
     * @param maintenance Maintenance version number.
     * @param stage Stage of development.
     * @param revTs Revision timestamp.
     * @param revHash Revision hash.
     */
    public GridProductVersion(byte major, byte minor, byte maintenance, String stage, long revTs, byte[] revHash) {
        if (revHash != null && revHash.length != 20)
            throw new IllegalArgumentException("Invalid length for SHA1 hash (must be 20): " + revHash.length);

        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
        this.stage = stage;
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
     * @return Stage of development.
     */
    public String stage() {
        return stage;
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

    /**
     * @param major Major version number.
     * @param minor Minor version number.
     * @param maintenance Maintenance version number.
     * @return {@code True} if this version is greater or equal than the one passed in.
     */
    public boolean greaterThanEqual(int major, int minor, int maintenance) {
        // NOTE: Unknown version is less than any other version.
        if (major == this.major)
            return minor == this.minor ? this.maintenance >= maintenance : this.minor > minor;
        else
            return this.major > major;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull GridProductVersion o) {
        // NOTE: Unknown version is less than any other version.
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

    /** {@inheritDoc} */
    public String toString() {
        String revTsStr = new SimpleDateFormat("yyyyMMdd").format(new Date(revTs * 1000));

        String hash = U.byteArray2HexString(revHash).toLowerCase();

        hash = hash.length() > 8 ? hash.substring(0, 8) : hash;

        return major + "." + minor + "." + maintenance + "#" + revTsStr + "-sha1:" + hash;
    }

    /**
     * Tries to parse product version from it's string representation.
     *
     * @param verStr String representation of version.
     * @return Product version.
     */
    @SuppressWarnings({"MagicConstant", "TypeMayBeWeakened"})
    public static GridProductVersion fromString(String verStr) {
        assert verStr != null;

        if (verStr.endsWith("-DEV")) // Development version, just cut it out.
            verStr = verStr.substring(0, verStr.length() - 4);

        Matcher match = VER_PATTERN.matcher(verStr);

        if (match.matches()) {
            try {
                byte major = Byte.parseByte(match.group(1));
                byte minor = Byte.parseByte(match.group(2));
                byte maintenance = Byte.parseByte(match.group(3));

                String stage = "";

                if (match.group(4) != null)
                    stage = match.group(6);

                long revTs = 0;

                if (match.group(9) != null)
                    revTs = Long.parseLong(match.group(10));

                byte[] revHash = null;

                if (match.group(11) != null)
                    revHash = U.decodeHex(match.group(12).toCharArray());

                return new GridProductVersion(major, minor, maintenance, stage, revTs, revHash);
            }
            catch (IllegalStateException | IndexOutOfBoundsException | NumberFormatException | GridException e) {
                throw new IllegalStateException("Failed to parse version: " + verStr, e);
            }
        }
        else
            throw new IllegalStateException("Failed to parse version: " + verStr);
    }
}
