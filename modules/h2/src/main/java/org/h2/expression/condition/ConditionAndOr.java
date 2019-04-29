/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * An 'and' or 'or' condition as in WHERE ID=1 AND NAME=?
 */
public class ConditionAndOr extends Condition {

    /**
     * The AND condition type as in ID=1 AND NAME='Hello'.
     */
    public static final int AND = 0;

    /**
     * The OR condition type as in ID=1 OR NAME='Hello'.
     */
    public static final int OR = 1;

    private final int andOrType;
    private Expression left, right;

    public ConditionAndOr(int andOrType, Expression left, Expression right) {
        this.andOrType = andOrType;
        this.left = left;
        this.right = right;
        if (left == null || right == null) {
            DbException.throwInternalError(left + " " + right);
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append('(');
        left.getSQL(builder, alwaysQuote);
        switch (andOrType) {
        case AND:
            builder.append("\n    AND ");
            break;
        case OR:
            builder.append("\n    OR ");
            break;
        default:
            throw DbException.throwInternalError("andOrType=" + andOrType);
        }
        return right.getSQL(builder, alwaysQuote).append(')');
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (andOrType == AND) {
            left.createIndexConditions(session, filter);
            right.createIndexConditions(session, filter);
        }
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        // (NOT (A OR B)): (NOT(A) AND NOT(B))
        // (NOT (A AND B)): (NOT(A) OR NOT(B))
        Expression l = left.getNotIfPossible(session);
        if (l == null) {
            l = new ConditionNot(left);
        }
        Expression r = right.getNotIfPossible(session);
        if (r == null) {
            r = new ConditionNot(right);
        }
        int reversed = andOrType == AND ? OR : AND;
        return new ConditionAndOr(reversed, l, r);
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        Value r;
        switch (andOrType) {
        case AND: {
            if (l != ValueNull.INSTANCE && !l.getBoolean()) {
                return l;
            }
            r = right.getValue(session);
            if (r != ValueNull.INSTANCE && !r.getBoolean()) {
                return r;
            }
            if (l == ValueNull.INSTANCE) {
                return l;
            }
            if (r == ValueNull.INSTANCE) {
                return r;
            }
            return ValueBoolean.TRUE;
        }
        case OR: {
            if (l.getBoolean()) {
                return l;
            }
            r = right.getValue(session);
            if (r.getBoolean()) {
                return r;
            }
            if (l == ValueNull.INSTANCE) {
                return l;
            }
            if (r == ValueNull.INSTANCE) {
                return r;
            }
            return ValueBoolean.FALSE;
        }
        default:
            throw DbException.throwInternalError("type=" + andOrType);
        }
    }

    @Override
    public Expression optimize(Session session) {
        // NULL handling: see wikipedia,
        // http://www-cs-students.stanford.edu/~wlam/compsci/sqlnulls
        left = left.optimize(session);
        right = right.optimize(session);
        int lc = left.getCost(), rc = right.getCost();
        if (rc < lc) {
            Expression t = left;
            left = right;
            right = t;
        }
        // this optimization does not work in the following case,
        // but NOT is optimized before:
        // CREATE TABLE TEST(A INT, B INT);
        // INSERT INTO TEST VALUES(1, NULL);
        // SELECT * FROM TEST WHERE NOT (B=A AND B=0); // no rows
        // SELECT * FROM TEST WHERE NOT (B=A AND B=0 AND A=0); // 1, NULL
        if (session.getDatabase().getSettings().optimizeTwoEquals &&
                andOrType == AND) {
            // try to add conditions (A=B AND B=1: add A=1)
            if (left instanceof Comparison && right instanceof Comparison) {
                Comparison compLeft = (Comparison) left;
                Comparison compRight = (Comparison) right;
                Expression added = compLeft.getAdditional(
                        session, compRight, true);
                if (added != null) {
                    added = added.optimize(session);
                    return new ConditionAndOr(AND, this, added);
                }
            }
        }

        if (andOrType == OR &&
                session.getDatabase().getSettings().optimizeOr) {
            // try to add conditions (A=B AND B=1: add A=1)
            if (left instanceof Comparison &&
                    right instanceof Comparison) {
                Comparison compLeft = (Comparison) left;
                Comparison compRight = (Comparison) right;
                Expression added = compLeft.getAdditional(
                        session, compRight, false);
                if (added != null) {
                    return added.optimize(session);
                }
            } else if (left instanceof ConditionIn &&
                    right instanceof Comparison) {
                Expression added = ((ConditionIn) left).
                        getAdditional((Comparison) right);
                if (added != null) {
                    return added.optimize(session);
                }
            } else if (right instanceof ConditionIn &&
                    left instanceof Comparison) {
                Expression added = ((ConditionIn) right).
                        getAdditional((Comparison) left);
                if (added != null) {
                    return added.optimize(session);
                }
            } else if (left instanceof ConditionInConstantSet &&
                    right instanceof Comparison) {
                Expression added = ((ConditionInConstantSet) left).
                        getAdditional(session, (Comparison) right);
                if (added != null) {
                    return added.optimize(session);
                }
            } else if (right instanceof ConditionInConstantSet &&
                    left instanceof Comparison) {
                Expression added = ((ConditionInConstantSet) right).
                        getAdditional(session, (Comparison) left);
                if (added != null) {
                    return added.optimize(session);
                }
            } else if (left instanceof ConditionAndOr &&
                    right instanceof ConditionAndOr ){
                ConditionAndOr condAORight = (ConditionAndOr)right;
                ConditionAndOr condAORLeft = (ConditionAndOr)left;
                Expression reduced = optimizeConditionAndOr(condAORLeft,condAORight);
                if(reduced != null){
                    return reduced.optimize(session);
                }
            }
        }
        // TODO optimization: convert .. OR .. to UNION if the cost is lower
        Value l = left.isConstant() ? left.getValue(session) : null;
        Value r = right.isConstant() ? right.getValue(session) : null;
        if (l == null && r == null) {
            return this;
        }
        if (l != null && r != null) {
            return ValueExpression.get(getValue(session));
        }
        switch (andOrType) {
        case AND:
            if (l != null) {
                if (l != ValueNull.INSTANCE && !l.getBoolean()) {
                    return ValueExpression.get(l);
                } else if (l.getBoolean()) {
                    return right;
                }
            } else if (r != null) {
                if (r != ValueNull.INSTANCE && !r.getBoolean()) {
                    return ValueExpression.get(r);
                } else if (r.getBoolean()) {
                    return left;
                }
            }
            break;
        case OR:
            if (l != null) {
                if (l.getBoolean()) {
                    return ValueExpression.get(l);
                } else if (l != ValueNull.INSTANCE) {
                    return right;
                }
            } else if (r != null) {
                if (r.getBoolean()) {
                    return ValueExpression.get(r);
                } else if (r != ValueNull.INSTANCE) {
                    return left;
                }
            }
            break;
        default:
            DbException.throwInternalError("type=" + andOrType);
        }
        return this;
    }

    @Override
    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        if (andOrType == AND) {
            left.addFilterConditions(filter, outerJoin);
            right.addFilterConditions(filter, outerJoin);
        } else {
            super.addFilterConditions(filter, outerJoin);
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        right.mapColumns(resolver, level, state);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
        right.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + right.getCost();
    }

    @Override
    public int getSubexpressionCount() {
        return 2;
    }

    @Override
    public Expression getSubexpression(int index) {
        switch (index) {
        case 0:
            return left;
        case 1:
            return right;
        default:
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * Optimize query according to the given condition. Example:
     * (A AND B) OR (C AND B), the new condition B AND (A OR C) is returned
     *
     * @param left the session
     * @param right the second condition
     * @return null or the third condition
     */
    private static Expression optimizeConditionAndOr(ConditionAndOr left, ConditionAndOr right) {
        if (left.andOrType != AND || right.andOrType != AND) {
            return null;
        }
        Expression leftLeft = left.getSubexpression(0), leftRight = left.getSubexpression(1);
        Expression rightLeft = right.getSubexpression(0), rightRight = right.getSubexpression(1);
        String leftLeftSQL = leftLeft.getSQL(true), rightLeftSQL = rightLeft.getSQL(true);
        Expression combinedExpression;
        if (leftLeftSQL.equals(rightLeftSQL)) {
            combinedExpression = new ConditionAndOr(OR, leftRight, rightRight);
            return new ConditionAndOr(AND, leftLeft, combinedExpression);
        }
        String rightRightSQL = rightRight.getSQL(true);
        if (leftLeftSQL.equals(rightRightSQL)) {
            combinedExpression = new ConditionAndOr(OR, leftRight, rightLeft);
            return new ConditionAndOr(AND, leftLeft, combinedExpression);
        }
        String leftRightSQL = leftRight.getSQL(true);
        if (leftRightSQL.equals(rightLeftSQL)) {
            combinedExpression = new ConditionAndOr(OR, leftLeft, rightRight);
            return new ConditionAndOr(AND, leftRight, combinedExpression);
        } else if (leftRightSQL.equals(rightRightSQL)) {
            combinedExpression = new ConditionAndOr(OR, leftLeft, rightLeft);
            return new ConditionAndOr(AND, leftRight, combinedExpression);
        }
        return null;
    }
}
