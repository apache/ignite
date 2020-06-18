package de.bwaldvogel.mongo.bson;

import java.util.Objects;
import java.util.UUID;

public final class LegacyUUID implements Comparable<LegacyUUID>, Bson {

    private static final long serialVersionUID = 1L;

    private final UUID uuid;

    public LegacyUUID(UUID uuid) {
        this.uuid = Objects.requireNonNull(uuid);
    }

    public LegacyUUID(long mostSigBits, long leastSigBits) {
        this(new UUID(mostSigBits, leastSigBits));
    }

    public UUID getUuid() {
        return uuid;
    }

    public static LegacyUUID fromString(String value) {
        return new LegacyUUID(UUID.fromString(value));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LegacyUUID that = (LegacyUUID) o;
        return getUuid().equals(that.getUuid());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUuid());
    }

    @Override
    public int compareTo(LegacyUUID o) {
        return getUuid().compareTo(o.getUuid());
    }

}
