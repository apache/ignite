package org.apache.ignite.internal.util;

import org.jetbrains.annotations.Nullable;

/**
 * Marks entity as assigned to a concrete grid.
 */
public interface GridByNameRelation {

    /**
     * Gets name of the grid this entity belongs to. Can be {@code null} which means the relation to the default grid.
     *
     * @return name of the grid.
     */
    @Nullable String getGridName();
}
