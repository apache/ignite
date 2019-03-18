/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.indexer;

import java.util.HashMap;

/**
 * This class replaces HTML entities in text (for example &uuml;) to the correct
 * character and vice versa.
 */
public class HtmlConverter {

    private static final HashMap<String, Character> CHAR_MAP =
            new HashMap<>();
    private static final HashMap<Character, String> CODE_MAP =
            new HashMap<>();

    private static final String[] CHARS = { "quot:34", "amp:38", "lt:60",
            "gt:62", "nbsp:160", "iexcl:161", "cent:162", "pound:163",
            "curren:164", "yen:165", "brvbar:166", "sect:167", "uml:168",
            "copy:169", "ordf:170", "laquo:171", "not:172", "shy:173",
            "reg:174", "macr:175", "deg:176", "plusmn:177", "sup2:178",
            "sup3:179", "acute:180", "micro:181", "para:182", "middot:183",
            "cedil:184", "sup1:185", "ordm:186", "raquo:187", "frac14:188",
            "frac12:189", "frac34:190", "iquest:191", "Agrave:192",
            "Aacute:193", "Acirc:194", "Atilde:195", "Auml:196", "Aring:197",
            "AElig:198", "Ccedil:199", "Egrave:200", "Eacute:201", "Ecirc:202",
            "Euml:203", "Igrave:204", "Iacute:205", "Icirc:206", "Iuml:207",
            "ETH:208", "Ntilde:209", "Ograve:210", "Oacute:211", "Ocirc:212",
            "Otilde:213", "Ouml:214", "times:215", "Oslash:216", "Ugrave:217",
            "Uacute:218", "Ucirc:219", "Uuml:220", "Yacute:221", "THORN:222",
            "szlig:223", "agrave:224", "aacute:225", "acirc:226", "atilde:227",
            "auml:228", "aring:229", "aelig:230", "ccedil:231", "egrave:232",
            "eacute:233", "ecirc:234", "euml:235", "igrave:236", "iacute:237",
            "icirc:238", "iuml:239", "eth:240", "ntilde:241", "ograve:242",
            "oacute:243", "ocirc:244", "otilde:245", "ouml:246", "divide:247",
            "oslash:248", "ugrave:249", "uacute:250", "ucirc:251", "uuml:252",
            "yacute:253", "thorn:254", "yuml:255", "Alpha:913", "alpha:945",
            "Beta:914", "beta:946", "Gamma:915", "gamma:947", "Delta:916",
            "delta:948", "Epsilon:917", "epsilon:949", "Zeta:918", "zeta:950",
            "Eta:919", "eta:951", "Theta:920", "theta:952", "Iota:921",
            "iota:953", "Kappa:922", "kappa:954", "Lambda:923", "lambda:955",
            "Mu:924", "mu:956", "Nu:925", "nu:957", "Xi:926", "xi:958",
            "Omicron:927", "omicron:959", "Pi:928", "pi:960", "Rho:929",
            "rho:961", "Sigma:931", "sigmaf:962", "sigma:963", "Tau:932",
            "tau:964", "Upsilon:933", "upsilon:965", "Phi:934", "phi:966",
            "Chi:935", "chi:967", "Psi:936", "psi:968", "Omega:937",
            "omega:969", "thetasym:977", "upsih:978", "piv:982", "forall:8704",
            "part:8706", "exist:8707", "empty:8709", "nabla:8711", "isin:8712",
            "notin:8713", "ni:8715", "prod:8719", "sum:8721", "minus:8722",
            "lowast:8727", "radic:8730", "prop:8733", "infin:8734", "ang:8736",
            "and:8743", "or:8744", "cap:8745", "cup:8746", "int:8747",
            "there4:8756", "sim:8764", "cong:8773", "asymp:8776", "ne:8800",
            "equiv:8801", "le:8804", "ge:8805", "sub:8834", "sup:8835",
            "nsub:8836", "sube:8838", "supe:8839", "oplus:8853", "otimes:8855",
            "perp:8869", "sdot:8901", "loz:9674", "lceil:8968", "rceil:8969",
            "lfloor:8970", "rfloor:8971", "lang:9001", "rang:9002",
            "larr:8592", "uarr:8593", "rarr:8594", "darr:8595", "harr:8596",
            "crarr:8629", "lArr:8656", "uArr:8657", "rArr:8658", "dArr:8659",
            "hArr:8660", "bull:8226", "prime:8242", "oline:8254", "frasl:8260",
            "weierp:8472", "image:8465", "real:8476", "trade:8482",
            "euro:8364", "alefsym:8501", "spades:9824", "clubs:9827",
            "hearts:9829", "diams:9830", "ensp:8194", "emsp:8195",
            "thinsp:8201", "zwnj:8204", "zwj:8205", "lrm:8206", "rlm:8207",
            "ndash:8211", "mdash:8212", "lsquo:8216", "rsquo:8217",
            "sbquo:8218", "ldquo:8220", "rdquo:8221", "bdquo:8222",
            "dagger:8224", "Dagger:8225", "hellip:8230", "permil:8240",
            "lsaquo:8249",            "rsaquo:8250" };

    private HtmlConverter() {
        // utility class
    }

    static {
        for (String token : CHARS) {
            int idx = token.indexOf(':');
            String key = token.substring(0, idx);
            int ch = Integer.parseInt(token.substring(idx + 1));
            Character character = Character.valueOf((char) ch);
            CHAR_MAP.put(key, character);
            CODE_MAP.put(character, key);
        }
    }

    /**
     * Convert a string to HTML by encoding all required characters.
     *
     * @param s the string
     * @return the HTML text
     */
    public static String convertStringToHtml(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() == 0) {
            return s;
        }
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            String token = CODE_MAP.get(ch);
            if (token == null) {
                if (ch < 128) {
                    buff.append(ch);
                } else {
                    buff.append('&');
                    buff.append('#');
                    buff.append((int) ch);
                    buff.append(';');
                }
            } else {
                buff.append('&');
                buff.append(token);
                buff.append(';');
            }
        }
        return buff.toString();
    }

    /**
     * Convert a HTML encoded text to a string.
     *
     * @param html the HTML text
     * @return the string
     */
    public static String convertHtmlToString(String html) {
        if (html == null) {
            return null;
        }
        if (html.length() == 0) {
            return html;
        }
        if (html.indexOf('&') < 0) {
            return html;
        }
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < html.length(); i++) {
            char ch = html.charAt(i);
            if (ch != '&') {
                buff.append(ch);
                continue;
            }
            int idx = html.indexOf(';', i + 1);
            if (idx < 0) {
                buff.append("???");
                continue;
            }
            String key = html.substring(i + 1, idx);
            Character repl;
            if (key.startsWith("#")) {
                if (key.startsWith("#x")) {
                    try {
                        int code = Integer.parseInt(key.substring(2), 16);
                        if (code < 0 || code > 0xffff) {
                            repl = null;
                        } else {
                            repl = Character.valueOf((char) code);
                        }
                    } catch (NumberFormatException e) {
                        repl = null;
                    }
                } else {
                    try {
                        int code = Integer.parseInt(key.substring(1));
                        if (code < 0 || code > 0xffff) {
                            repl = null;
                        } else {
                            repl = Character.valueOf((char) code);
                        }
                    } catch (NumberFormatException e) {
                        repl = null;
                    }
                }
            } else {
                repl = CHAR_MAP.get(key);
            }
            if (repl == null) {
                buff.append("???" + key + "???");
                continue;
            }
            buff.append(repl.charValue());
            i = idx;
        }
        return buff.toString();
    }

}
