/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

/**
 * Result of update command with optional generated keys.
 */
public class ResultWithGeneratedKeys {
    /**
     * Result of update command with generated keys;
     */
    public static final class WithKeys extends ResultWithGeneratedKeys {
        private final ResultInterface generatedKeys;

        /**
         * Creates a result with update count and generated keys.
         *
         * @param updateCount
         *            update count
         * @param generatedKeys
         *            generated keys
         */
        public WithKeys(int updateCount, ResultInterface generatedKeys) {
            super(updateCount);
            this.generatedKeys = generatedKeys;
        }

        @Override
        public ResultInterface getGeneratedKeys() {
            return generatedKeys;
        }
    }

    /**
     * Returns a result with only update count.
     *
     * @param updateCount
     *            update count
     * @return the result.
     */
    public static ResultWithGeneratedKeys of(int updateCount) {
        return new ResultWithGeneratedKeys(updateCount);
    }

    private final int updateCount;

    ResultWithGeneratedKeys(int updateCount) {
        this.updateCount = updateCount;
    }

    /**
     * Returns generated keys, or {@code null}.
     *
     * @return generated keys, or {@code null}
     */
    public ResultInterface getGeneratedKeys() {
        return null;
    }

    /**
     * Returns update count.
     *
     * @return update count
     */
    public int getUpdateCount() {
        return updateCount;
    }

}
