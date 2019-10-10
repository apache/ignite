/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

import java.util.ArrayList;

/**
 * A statement.
 */
public interface Statement {

    void setMethod(MethodObj method);
    boolean isEnd();

    /**
     * Get the C++ code.
     *
     * @return the C++ code
     */
    String asString();

}

/**
 * The base class for statements.
 */
abstract class StatementBase implements Statement {

    @Override
    public boolean isEnd() {
        return false;
    }

}

/**
 * A "return" statement.
 */
class ReturnStatement extends StatementBase {

    /**
     * The return expression.
     */
    Expr expr;

    private MethodObj method;

    @Override
    public void setMethod(MethodObj method) {
        this.method = method;
    }

    @Override
    public String asString() {
        if (expr == null) {
            return "return;";
        }
        Type returnType = method.returnType;
        expr.setType(returnType);
        if (!expr.getType().isObject()) {
            return "return " + expr.asString() + ";";
        }
        if (returnType.refCount) {
            return "return " + expr.getType().asString() + "(" + expr.asString() + ");";
        }
        return "return " + expr.asString() + ";";
    }

}

/**
 * A "do .. while" statement.
 */
class DoWhileStatement extends StatementBase {

    /**
     * The condition.
     */
    Expr condition;

    /**
     * The execution block.
     */
    Statement block;

    @Override
    public void setMethod(MethodObj method) {
        block.setMethod(method);
    }

    @Override
    public String asString() {
        return "do {\n" + block + "} while (" + condition.asString() + ");";
    }

}

/**
 * A "continue" statement.
 */
class ContinueStatement extends StatementBase {

    @Override
    public void setMethod(MethodObj method) {
        // ignore
    }

    @Override
    public String asString() {
        return "continue;";
    }

}

/**
 * A "break" statement.
 */
class BreakStatement extends StatementBase {

    @Override
    public void setMethod(MethodObj method) {
        // ignore
    }

    @Override
    public String asString() {
        return "break;";
    }

}

/**
 * An empty statement.
 */
class EmptyStatement extends StatementBase {

    @Override
    public void setMethod(MethodObj method) {
        // ignore
    }

    @Override
    public String asString() {
        return ";";
    }

}

/**
 * A "switch" statement.
 */
class SwitchStatement extends StatementBase {

    private StatementBlock defaultBlock;
    private final ArrayList<Expr> cases = new ArrayList<>();
    private final ArrayList<StatementBlock> blocks =
            new ArrayList<>();
    private final Expr expr;

    public SwitchStatement(Expr expr) {
        this.expr = expr;
    }

    @Override
    public void setMethod(MethodObj method) {
        defaultBlock.setMethod(method);
        for (StatementBlock b : blocks) {
            b.setMethod(method);
        }
    }

    @Override
    public String asString() {
        StringBuilder buff = new StringBuilder();
        buff.append("switch (").append(expr.asString()).append(") {\n");
        for (int i = 0; i < cases.size(); i++) {
            buff.append("case " + cases.get(i).asString() + ":\n");
            buff.append(blocks.get(i).toString());
        }
        if (defaultBlock != null) {
            buff.append("default:\n");
            buff.append(defaultBlock.toString());
        }
        buff.append("}");
        return buff.toString();
    }

    public void setDefaultBlock(StatementBlock block) {
        this.defaultBlock = block;
    }

    /**
     * Add a case.
     *
     * @param expr the case expression
     * @param block the execution block
     */
    public void addCase(Expr expr, StatementBlock block) {
        cases.add(expr);
        blocks.add(block);
    }

}

/**
 * An expression statement.
 */
class ExprStatement extends StatementBase {

    private final Expr expr;

    public ExprStatement(Expr expr) {
        this.expr = expr;
    }

    @Override
    public void setMethod(MethodObj method) {
        // ignore
    }

    @Override
    public String asString() {
        return expr.asString() + ";";
    }

}

/**
 * A "while" statement.
 */
class WhileStatement extends StatementBase {

    /**
     * The condition.
     */
    Expr condition;

    /**
     * The execution block.
     */
    Statement block;

    @Override
    public void setMethod(MethodObj method) {
        block.setMethod(method);
    }

    @Override
    public String asString() {
        String w = "while (" + condition.asString() + ")";
        String s = block.toString();
        return w + "\n" + s;
    }

}

/**
 * An "if" statement.
 */
class IfStatement extends StatementBase {

    /**
     * The condition.
     */
    Expr condition;

    /**
     * The execution block.
     */
    Statement block;

    /**
     * The else block.
     */
    Statement elseBlock;

    @Override
    public void setMethod(MethodObj method) {
        block.setMethod(method);
        if (elseBlock != null) {
            elseBlock.setMethod(method);
        }
    }

    @Override
    public String asString() {
        String w = "if (" + condition.asString() + ") {\n";
        String s = block.asString();
        if (elseBlock != null) {
            s += "} else {\n" + elseBlock.asString();
        }
        return w + s + "}";
    }

}

/**
 * A "for" statement.
 */
class ForStatement extends StatementBase {

    /**
     * The init block.
     */
    Statement init;

    /**
     * The condition.
     */
    Expr condition;

    /**
     * The main loop block.
     */
    Statement block;

    /**
     * The update list.
     */
    ArrayList<Expr> updates = new ArrayList<>();

    /**
     * The type of the iterable.
     */
    Type iterableType;

    /**
     * The iterable variable name.
     */
    String iterableVariable;

    /**
     * The iterable expression.
     */
    Expr iterable;

    @Override
    public void setMethod(MethodObj method) {
        block.setMethod(method);
    }

    @Override
    public String asString() {
        StringBuilder buff = new StringBuilder();
        buff.append("for (");
        if (iterableType != null) {
            Type it = iterable.getType();
            if (it != null && it.arrayLevel > 0) {
                String idx = "i_" + iterableVariable;
                buff.append("int " + idx + " = 0; " +
                        idx + " < " + iterable.asString() + "->length(); " +
                        idx + "++");
                buff.append(") {\n");
                buff.append(JavaParser.indent(iterableType +
                        " " + iterableVariable + " = " +
                        iterable.asString() + "->at("+ idx +");\n"));
                buff.append(block.toString()).append("}");
            } else {
                // TODO iterate over a collection
                buff.append(iterableType).append(' ');
                buff.append(iterableVariable).append(": ");
                buff.append(iterable);
                buff.append(") {\n");
                buff.append(block.toString()).append("}");
            }
        } else {
            buff.append(init.asString());
            buff.append(" ").append(condition.asString()).append("; ");
            for (int i = 0; i < updates.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(updates.get(i).asString());
            }
            buff.append(") {\n");
            buff.append(block.asString()).append("}");
        }
        return buff.toString();
    }

}

/**
 * A statement block.
 */
class StatementBlock extends StatementBase {

    /**
     * The list of instructions.
     */
    final ArrayList<Statement> instructions = new ArrayList<>();

    @Override
    public void setMethod(MethodObj method) {
        for (Statement s : instructions) {
            s.setMethod(method);
        }
    }

    @Override
    public String asString() {
        StringBuilder buff = new StringBuilder();
        for (Statement s : instructions) {
            if (s.isEnd()) {
                break;
            }
            buff.append(JavaParser.indent(s.asString()));
        }
        return buff.toString();
    }

}

/**
 * A variable declaration.
 */
class VarDecStatement extends StatementBase {

    /**
     * The type.
     */
    Type type;

    private final ArrayList<String> variables = new ArrayList<>();
    private final ArrayList<Expr> values = new ArrayList<>();

    @Override
    public void setMethod(MethodObj method) {
        // ignore
    }

    @Override
    public String asString() {
        StringBuilder buff = new StringBuilder();
        buff.append(type.asString()).append(' ');
        StringBuilder assign = new StringBuilder();
        for (int i = 0; i < variables.size(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            String varName = variables.get(i);
            buff.append(varName);
            Expr value = values.get(i);
            if (value != null) {
                if (!value.getType().isObject()) {
                    buff.append(" = ").append(value.asString());
                } else {
                    value.setType(type);
                    assign.append(varName).append(" = ").append(value.asString()).append(";\n");
                }
            }
        }
        buff.append(";");
        if (assign.length() > 0) {
            buff.append("\n");
            buff.append(assign);
        }
        return buff.toString();
    }

    /**
     * Add a variable.
     *
     * @param name the variable name
     * @param value the init value
     */
    public void addVariable(String name, Expr value) {
        variables.add(name);
        values.add(value);
    }

}

/**
 * A native statement.
 */
class StatementNative extends StatementBase {

    private final String code;

    StatementNative(String code) {
        this.code = code;
    }

    @Override
    public void setMethod(MethodObj method) {
        // ignore
    }

    @Override
    public String asString() {
        return code;
    }

    @Override
    public boolean isEnd() {
        return code.equals("return;");
    }

}

