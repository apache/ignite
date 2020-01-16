package org.apache.ignite.testframework.junits.common;

/** Enum describe different relationship between partition and key. */
public enum TypePartitionForKey {
    // Partition is primary for key.
    PRIMARY,
    // Partition is backup for key.
    BACKUP,
    // Partition not primary and not backup for key.
    OTHER;
}
