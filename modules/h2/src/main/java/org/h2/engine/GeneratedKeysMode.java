/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Modes of generated keys' gathering.
 */
public final class GeneratedKeysMode {

    /**
     * Generated keys are not needed.
     */
    public static final int NONE = 0;

    /**
     * Generated keys should be configured automatically.
     */
    public static final int AUTO = 1;

    /**
     * Use specified column indices to return generated keys from.
     */
    public static final int COLUMN_NUMBERS = 2;

    /**
     * Use specified column names to return generated keys from.
     */
    public static final int COLUMN_NAMES = 3;

    /**
     * Determines mode of generated keys' gathering.
     *
     * @param generatedKeysRequest
     *            {@code false} if generated keys are not needed, {@code true} if
     *            generated keys should be configured automatically, {@code int[]}
     *            to specify column indices to return generated keys from, or
     *            {@code String[]} to specify column names to return generated keys
     *            from
     * @return mode for the specified generated keys request
     */
    public static int valueOf(Object generatedKeysRequest) {
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return NONE;
        }
        if (Boolean.TRUE.equals(generatedKeysRequest)) {
            return AUTO;
        }
        if (generatedKeysRequest instanceof int[]) {
            return COLUMN_NUMBERS;
        }
        if (generatedKeysRequest instanceof String[]) {
            return COLUMN_NAMES;
        }
        throw DbException.get(ErrorCode.INVALID_VALUE_2,
                generatedKeysRequest == null ? "null" : generatedKeysRequest.toString());
    }

    private GeneratedKeysMode() {
    }

}
