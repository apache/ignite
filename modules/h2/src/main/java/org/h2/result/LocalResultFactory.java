/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Session;
import org.h2.expression.Expression;

/**
 * Creates local result.
 */
public abstract class LocalResultFactory {
    /**
     * Default implementation of local result factory.
     */
    public static final LocalResultFactory DEFAULT = new DefaultLocalResultFactory();

    /**
     * Create a local result object.
     *
     * @param session the session
     * @param expressions the expression array
     * @param visibleColumnCount the number of visible columns
     * @return object to collect local result.
     */
    public abstract LocalResult create(Session session, Expression[] expressions, int visibleColumnCount);

    /**
     * Create a local result object.
     * @return object to collect local result.
     */
    public abstract LocalResult create();

    /**
     * Default implementation of local result factory.
     */
    private static final class DefaultLocalResultFactory extends LocalResultFactory {
        /**
         *
         */
        DefaultLocalResultFactory() {
            //No-op.
        }

        @Override
        public LocalResult create(Session session, Expression[] expressions, int visibleColumnCount) {
            return new LocalResultImpl(session, expressions, visibleColumnCount);
        }

        @Override
        public LocalResult create() {
            return new LocalResultImpl();
        }
    }
}
