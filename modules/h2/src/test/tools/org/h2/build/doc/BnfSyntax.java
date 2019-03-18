/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.util.ArrayList;
import java.util.StringTokenizer;
import org.h2.bnf.Bnf;
import org.h2.bnf.BnfVisitor;
import org.h2.bnf.Rule;
import org.h2.bnf.RuleFixed;
import org.h2.bnf.RuleHead;
import org.h2.util.StringUtils;

/**
 * A BNF visitor that generates BNF in HTML form.
 */
public class BnfSyntax implements BnfVisitor {

    private String html;

    /**
     * Get the HTML syntax for the given syntax.
     *
     * @param bnf the BNF
     * @param syntaxLines the syntax
     * @return the HTML
     */
    public String getHtml(Bnf bnf, String syntaxLines) {
        syntaxLines = StringUtils.replaceAll(syntaxLines, "\n    ", "\n");
        StringTokenizer tokenizer = Bnf.getTokenizer(syntaxLines);
        StringBuilder buff = new StringBuilder();
        while (tokenizer.hasMoreTokens()) {
            String s = tokenizer.nextToken();
            if (s.length() == 1 || StringUtils.toUpperEnglish(s).equals(s)) {
                buff.append(StringUtils.xmlText(s));
                continue;
            }
            buff.append(getLink(bnf, s));
        }
        String s = buff.toString();
        // ensure it works within XHTML comments
        s = StringUtils.replaceAll(s, "--", "&#45;-");
        return s;
    }

    /**
     * Get the HTML link to the given token.
     *
     * @param bnf the BNF
     * @param token the token
     * @return the HTML link
     */
    String getLink(Bnf bnf, String token) {
        RuleHead found = null;
        String key = Bnf.getRuleMapKey(token);
        for (int i = 0; i < token.length(); i++) {
            String test = StringUtils.toLowerEnglish(key.substring(i));
            RuleHead r = bnf.getRuleHead(test);
            if (r != null) {
                found = r;
                break;
            }
        }
        if (found == null) {
            return token;
        }
        String page = "grammar.html";
        if (found.getSection().startsWith("Data Types")) {
            page = "datatypes.html";
        } else if (found.getSection().startsWith("Functions")) {
            page = "functions.html";
        } else if (token.equals("@func@")) {
            return "<a href=\"functions.html\">Function</a>";
        } else if (found.getRule() instanceof RuleFixed) {
            found.getRule().accept(this);
            return html;
        }
        String link = found.getTopic().toLowerCase().replace(' ', '_');
        link = page + "#" + StringUtils.urlEncode(link);
        return "<a href=\"" + link + "\">" + token + "</a>";
    }

    @Override
    public void visitRuleElement(boolean keyword, String name, Rule link) {
        // not used
    }

    @Override
    public void visitRuleFixed(int type) {
        html = BnfRailroad.getHtmlText(type);
    }

    @Override
    public void visitRuleList(boolean or, ArrayList<Rule> list) {
        // not used
    }

    @Override
    public void visitRuleOptional(Rule rule) {
        // not used
    }

    @Override
    public void visitRuleRepeat(boolean comma, Rule rule) {
        // not used
    }

}
