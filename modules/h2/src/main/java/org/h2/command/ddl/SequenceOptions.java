/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.schema.Sequence;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Sequence options.
 */
public class SequenceOptions {

    private Expression start;

    private Expression increment;

    private Expression maxValue;

    private Expression minValue;

    private Boolean cycle;

    private Expression cacheSize;

    private static Long getLong(Session session, Expression expr) {
        if (expr != null) {
            Value value = expr.optimize(session).getValue(session);
            if (value != ValueNull.INSTANCE) {
                return value.getLong();
            }
        }
        return null;
    }

    /**
     * Gets start value.
     *
     * @param session The session to calculate the value.
     * @return start value or {@code null} if value is not defined.
     */
    public Long getStartValue(Session session) {
        return getLong(session, start);
    }

    /**
     * Sets start value expression.
     *
     * @param start START WITH value expression.
     */
    public void setStartValue(Expression start) {
        this.start = start;
    }

    /**
     * Gets increment value.
     *
     * @param session The session to calculate the value.
     * @return increment value or {@code null} if value is not defined.
     */
    public Long getIncrement(Session session) {
        return getLong(session, increment);
    }

    /**
     * Sets increment value expression.
     *
     * @param increment INCREMENT BY value expression.
     */
    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    /**
     * Gets max value.
     *
     * @param sequence the sequence to get default max value.
     * @param session The session to calculate the value.
     * @return max value when the MAXVALUE expression is set, otherwise returns default max value.
     */
    public Long getMaxValue(Sequence sequence, Session session) {
        if (maxValue == ValueExpression.getNull() && sequence != null) {
            return Sequence.getDefaultMaxValue(getCurrentStart(sequence, session),
                    increment != null ? getIncrement(session) : sequence.getIncrement());
        }
        return getLong(session, maxValue);
    }

    /**
     * Sets max value expression.
     *
     * @param maxValue MAXVALUE expression.
     */
    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    /**
     * Gets min value.
     *
     * @param sequence the sequence to get default min value.
     * @param session The session to calculate the value.
     * @return min value when the MINVALUE expression is set, otherwise returns default min value.
     */
    public Long getMinValue(Sequence sequence, Session session) {
        if (minValue == ValueExpression.getNull() && sequence != null) {
            return Sequence.getDefaultMinValue(getCurrentStart(sequence, session),
                    increment != null ? getIncrement(session) : sequence.getIncrement());
        }
        return getLong(session, minValue);
    }

    /**
     * Sets min value expression.
     *
     * @param minValue MINVALUE expression.
     */
    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    /**
     * Gets cycle flag.
     *
     * @return cycle flag value or {@code null} if value is not defined.
     */
    public Boolean getCycle() {
        return cycle;
    }

    /**
     * Sets cycle flag.
     *
     * @param cycle flag value.
     */
    public void setCycle(Boolean cycle) {
        this.cycle = cycle;
    }

    /**
     * Gets cache size.
     *
     * @param session The session to calculate the value.
     * @return cache size or {@code null} if value is not defined.
     */
    public Long getCacheSize(Session session) {
        return getLong(session, cacheSize);
    }

    /**
     * Sets cache size.
     *
     * @param cacheSize cache size.
     */
    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

    boolean isRangeSet() {
        return start != null || minValue != null || maxValue != null || increment != null;
    }

    private long getCurrentStart(Sequence sequence, Session session) {
        return start != null ? getStartValue(session) : sequence.getCurrentValue() + sequence.getIncrement();
    }
}
