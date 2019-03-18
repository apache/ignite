/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.util.ArrayList;
import org.h2.bnf.Bnf;
import org.h2.bnf.BnfVisitor;
import org.h2.bnf.Rule;
import org.h2.bnf.RuleFixed;
import org.h2.util.StringUtils;

/**
 * A BNF visitor that generates HTML railroad diagrams.
 */
public class BnfRailroad implements BnfVisitor {

    private static final boolean RAILROAD_DOTS = true;

    private BnfSyntax syntaxVisitor;
    private Bnf config;
    private String html;

    /**
     * Generate the HTML for the given syntax.
     *
     * @param bnf the BNF parser
     * @param syntaxLines the syntax
     * @return the HTML
     */
    public String getHtml(Bnf bnf, String syntaxLines) {
        syntaxVisitor = new BnfSyntax();
        this.config = bnf;
        syntaxLines = StringUtils.replaceAll(syntaxLines, "\n    ", " ");
        String[] syntaxList = StringUtils.arraySplit(syntaxLines, '\n', true);
        StringBuilder buff = new StringBuilder();
        for (String s : syntaxList) {
            bnf.visit(this, s);
            html = StringUtils.replaceAll(html, "</code></td>" +
                    "<td class=\"d\"><code class=\"c\">", " ");
            if (buff.length() > 0) {
                buff.append("<br />");
            }
            buff.append(html);
        }
        return buff.toString();
    }

    @Override
    public void visitRuleElement(boolean keyword, String name, Rule link) {
        String x;
        if (keyword) {
            x = StringUtils.xmlText(name.trim());
        } else {
            x = syntaxVisitor.getLink(config, name.trim());
        }
        html = "<code class=\"c\">" + x + "</code>";
    }

    @Override
    public void visitRuleRepeat(boolean comma, Rule rule) {
        StringBuilder buff = new StringBuilder();
        if (RAILROAD_DOTS) {
            buff.append("<code class=\"c\">");
            if (comma) {
                buff.append(", ");
            }
            buff.append("...</code>");
        } else {
            buff.append("<table class=\"railroad\">");
            buff.append("<tr class=\"railroad\"><td class=\"te\"></td>");
            buff.append("<td class=\"d\">");
            rule.accept(this);
            buff.append(html);
            buff.append("</td><td class=\"ts\"></td></tr>");
            buff.append("<tr class=\"railroad\"><td class=\"ls\"></td>");
            buff.append("<td class=\"d\">&nbsp;</td>");
            buff.append("<td class=\"le\"></td></tr></table>");
        }
        html = buff.toString();
    }

    @Override
    public void visitRuleFixed(int type) {
        html = getHtmlText(type);
    }

    /**
     * Get the HTML text for the given fixed rule.
     *
     * @param type the fixed rule type
     * @return the HTML text
     */
    static String getHtmlText(int type) {
        switch (type) {
        case RuleFixed.YMD:
            return "2000-01-01";
        case RuleFixed.HMS:
            return "12:00:00";
        case RuleFixed.NANOS:
            return "000000000";
        case RuleFixed.ANY_UNTIL_EOL:
        case RuleFixed.ANY_EXCEPT_SINGLE_QUOTE:
        case RuleFixed.ANY_EXCEPT_DOUBLE_QUOTE:
        case RuleFixed.ANY_WORD:
        case RuleFixed.ANY_EXCEPT_2_DOLLAR:
        case RuleFixed.ANY_UNTIL_END: {
            return "anything";
        }
        case RuleFixed.HEX_START:
            return "0x";
        case RuleFixed.CONCAT:
            return "||";
        case RuleFixed.AZ_UNDERSCORE:
            return "A-Z | _";
        case RuleFixed.AF:
            return "A-F";
        case RuleFixed.DIGIT:
            return "0-9";
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
        StringBuilder buff = new StringBuilder();
        if (or) {
            buff.append("<table class=\"railroad\">");
            int i = 0;
            for (Rule r : list) {
                String a = i == 0 ? "t" : i == list.size() - 1 ? "l" : "k";
                i++;
                buff.append("<tr class=\"railroad\"><td class=\"" +
                        a + "s\"></td><td class=\"d\">");
                r.accept(this);
                buff.append(html);
                buff.append("</td><td class=\"" + a + "e\"></td></tr>");
            }
            buff.append("</table>");
        } else {
            buff.append("<table class=\"railroad\">");
            buff.append("<tr class=\"railroad\">");
            for (Rule r : list) {
                buff.append("<td class=\"d\">");
                r.accept(this);
                buff.append(html);
                buff.append("</td>");
            }
            buff.append("</tr></table>");
        }
        html = buff.toString();
    }

    @Override
    public void visitRuleOptional(Rule rule) {
        StringBuilder buff = new StringBuilder();
        buff.append("<table class=\"railroad\">");
        buff.append("<tr class=\"railroad\"><td class=\"ts\"></td>" +
                "<td class=\"d\">&nbsp;</td><td class=\"te\"></td></tr>");
        buff.append("<tr class=\"railroad\">" +
                "<td class=\"ls\"></td><td class=\"d\">");
        rule.accept(this);
        buff.append(html);
        buff.append("</td><td class=\"le\"></td></tr></table>");
        html = buff.toString();
    }

}
