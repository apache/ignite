/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.ArrayList;

/**
 * The visitor interface for BNF rules.
 */
public interface BnfVisitor {

    /**
     * Visit a rule element.
     *
     * @param keyword whether this is a keyword
     * @param name the element name
     * @param link the linked rule if it's not a keyword
     */
    void visitRuleElement(boolean keyword, String name, Rule link);

    /**
     * Visit a repeat rule.
     *
     * @param comma whether the comma is repeated as well
     * @param rule the element to repeat
     */
    void visitRuleRepeat(boolean comma, Rule rule);

    /**
     * Visit a fixed rule.
     *
     * @param type the type
     */
    void visitRuleFixed(int type);

    /**
     * Visit a rule list.
     *
     * @param or true for OR, false for AND
     * @param list the rules
     */
    void visitRuleList(boolean or, ArrayList<Rule> list);

    /**
     * Visit an optional rule.
     *
     * @param rule the rule
     */
    void visitRuleOptional(Rule rule);

}
