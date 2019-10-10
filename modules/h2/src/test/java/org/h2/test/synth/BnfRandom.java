/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.util.ArrayList;
import java.util.Random;
import org.h2.bnf.Bnf;
import org.h2.bnf.BnfVisitor;
import org.h2.bnf.Rule;
import org.h2.bnf.RuleFixed;
import org.h2.bnf.RuleHead;
import org.h2.util.New;

/**
 * A BNF visitor that generates a random SQL statement.
 */
public class BnfRandom implements BnfVisitor {

    private static final boolean SHOW_SYNTAX = false;

    private final Random random = new Random();
    private final ArrayList<RuleHead> statements = New.arrayList();

    private int level;
    private String sql;

    public BnfRandom() throws Exception {
        Bnf config = Bnf.getInstance(null);
        config.addAlias("procedure", "@func@");
        config.linkStatements();

        ArrayList<RuleHead> all = config.getStatements();

        // go backwards so we can append at the end
        for (int i = all.size() - 1; i >= 0; i--) {
            RuleHead r = all.get(i);
            String topic = r.getTopic().toLowerCase();
            int weight = 0;
            if (topic.equals("select")) {
                weight = 10;
            } else if (topic.equals("create table")) {
                weight = 20;
            } else if (topic.equals("insert")) {
                weight = 5;
            } else if (topic.startsWith("update")) {
                weight = 3;
            } else if (topic.startsWith("delete")) {
                weight = 3;
            } else if (topic.startsWith("drop")) {
                weight = 2;
            }
            if (SHOW_SYNTAX) {
                System.out.println(r.getTopic());
            }
            for (int j = 0; j < weight; j++) {
                statements.add(r);
            }
        }
    }

    public String getRandomSQL() {
        int sid = random.nextInt(statements.size());

        RuleHead r = statements.get(sid);
        level = 0;
        r.getRule().accept(this);
        sql = sql.trim();

        if (sql.length() > 0) {
            if (sql.indexOf("TRACE_LEVEL_") < 0
                    && sql.indexOf("COLLATION") < 0
                    && sql.indexOf("SCRIPT ") < 0
                    && sql.indexOf("CSVWRITE") < 0
                    && sql.indexOf("BACKUP") < 0
                    && sql.indexOf("DB_CLOSE_DELAY") < 0) {
                if (SHOW_SYNTAX) {
                    System.out.println("  " + sql);
                }
                return sql;
            }
        }
        return null;
    }

    @Override
    public void visitRuleElement(boolean keyword, String name, Rule link) {
        if (keyword) {
            if (name.startsWith(";")) {
                sql = "";
            } else {
                sql = name.length() > 1 ? " " + name + " " : name;
            }
        } else if (link != null) {
            level++;
            link.accept(this);
            level--;
        } else {
            throw new AssertionError(name);
        }
    }

    @Override
    public void visitRuleFixed(int type) {
        sql = getRandomFixed(type);
    }

    private String getRandomFixed(int type) {
        Random r = random;
        switch (type) {
        case RuleFixed.YMD:
            return (1800 + r.nextInt(200)) + "-" +
                (1 + r.nextInt(12)) + "-" + (1 + r.nextInt(31));
        case RuleFixed.HMS:
            return (r.nextInt(24)) + "-" + (r.nextInt(60)) + "-" + (r.nextInt(60));
        case RuleFixed.NANOS:
            return "" + (r.nextInt(100000) + r.nextInt(10000));
        case RuleFixed.ANY_UNTIL_EOL:
        case RuleFixed.ANY_EXCEPT_SINGLE_QUOTE:
        case RuleFixed.ANY_EXCEPT_DOUBLE_QUOTE:
        case RuleFixed.ANY_WORD:
        case RuleFixed.ANY_EXCEPT_2_DOLLAR:
        case RuleFixed.ANY_UNTIL_END: {
            StringBuilder buff = new StringBuilder();
            int len = r.nextBoolean() ? 1 : r.nextInt(5);
            for (int i = 0; i < len; i++) {
                buff.append((char) ('A' + r.nextInt('C' - 'A')));
            }
            return buff.toString();
        }
        case RuleFixed.HEX_START:
            return "0x";
        case RuleFixed.CONCAT:
            return "||";
        case RuleFixed.AZ_UNDERSCORE:
            return "" + (char) ('A' + r.nextInt('C' - 'A'));
        case RuleFixed.AF:
            return "" + (char) ('A' + r.nextInt('F' - 'A'));
        case RuleFixed.DIGIT:
            return "" + (char) ('0' + r.nextInt(10));
        case RuleFixed.OPEN_BRACKET:
            return "[";
        case RuleFixed.CLOSE_BRACKET:
            return "]";
        default:
            throw new AssertionError("type="+type);
        }
    }

    @Override
    public void visitRuleList(boolean or, ArrayList<Rule> list) {
        if (or) {
            if (level > 10) {
                if (level > 1000) {
                    // better than stack overflow
                    throw new AssertionError();
                }
                list.get(0).accept(this);
                return;
            }
            int idx = random.nextInt(list.size());
            level++;
            list.get(idx).accept(this);
            level--;
            return;
        }
        StringBuilder buff = new StringBuilder();
        level++;
        for (Rule r : list) {
            r.accept(this);
            buff.append(sql);
        }
        level--;
        sql = buff.toString();
    }

    @Override
    public void visitRuleOptional(Rule rule) {
        if (level > 10 ? random.nextInt(level) == 1 : random.nextInt(4) == 1) {
            level++;
            rule.accept(this);
            level--;
            return;
        }
        sql = "";
    }

    @Override
    public void visitRuleRepeat(boolean comma, Rule rule) {
        rule.accept(this);
    }

    public void setSeed(int seed) {
        random.setSeed(seed);
    }

    public int getStatementCount() {
        return statements.size();
    }

}
