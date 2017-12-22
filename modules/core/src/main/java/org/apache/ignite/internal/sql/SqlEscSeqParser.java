package org.apache.ignite.internal.sql;

import org.apache.ignite.IgniteIllegalStateException;

/** FIXME */
public class SqlEscSeqParser {

    /** FIXME */
    public enum Mode {
        /** FIXME */
        START,
        /** FIXME */
        CHAR_BY_CODE,
        /** FIXME */
        OTHER
    }

    /** FIXME */
    private Mode mode;

    /** FIXME */
    private int minLen;

    /** FIXME */
    private int maxLen;

    /** FIXME */
    private int radix;

    /** FIXME */
    private boolean needMoreInput;

    /** FIXME */
    private final StringBuffer input = new StringBuffer();

    /** FIXME */
    public SqlEscSeqParser() {
        this.mode = Mode.START;
        this.needMoreInput = true;
    }

    /** FIXME */
    public boolean accept(char c) {

        needMoreInput = (mode == Mode.START) ? acceptStart(c) : acceptOther(c);

        return needMoreInput;
    }

    /** FIXME */
    private boolean acceptStart(char c) {
        if (!needMoreInput)
            throw new IgniteIllegalStateException("Internal error");

        switch (c) {
            case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7':
                mode = Mode.CHAR_BY_CODE;
                minLen = 1;
                maxLen = 3;
                radix = 8;
                input.append(c);
                break;

            case 'x': case 'X':
                mode = Mode.CHAR_BY_CODE;
                minLen = 1;
                maxLen = 2;
                radix = 16;
                break;

            case 'u':
                mode = Mode.CHAR_BY_CODE;
                minLen = 4;
                maxLen = 4;
                radix = 16;
                break;

            case 'U':
                mode = Mode.CHAR_BY_CODE;
                minLen = 8;
                maxLen = 8;
                radix = 16;
                break;

            default:
                mode = Mode.OTHER;
                minLen = 1;
                maxLen = 1;
                radix = -1;
                return acceptOther(c);
        }

        return true;
    }

    /** FIXME */
    private boolean acceptOther(char c) {
        int inputLen = input.length();

        if (mode == Mode.CHAR_BY_CODE) {
            if (!isDigit(c)) {
                if (inputLen < minLen)
                    throw new NumberFormatException("Illegal " + radix + "-base character: " + c);

                return false;
            }
        }

        if (inputLen <= maxLen)
            input.append(c);

        return inputLen < maxLen;
    }

    /** FIXME */
    private boolean isDigit(char c) {
        if (radix < 10)
            return c >= '0' && c < ('0' + radix);
        else
            return (c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'a' + (radix - 10)) ||
                (c >= 'A' && c <= 'A' + (radix - 10));
    }

    /** FIXME */
    public String convertedStr() {
        if (mode == Mode.START || input.length() < minLen)
            throw new IgniteIllegalStateException("Internal error");

        switch (mode) {
            case OTHER:
                return Character.toString(convertEscSeqChar(input.charAt(0)));

            case CHAR_BY_CODE:
                // FIXME: Unicode
                return Character.toString((char) Integer.parseInt(input.toString(), radix));
        }

        throw new IgniteIllegalStateException("Internal error");
    }

    /**
     * Converts second character from escape sequence to actual character
     *
     * @param c The character after the backquote.
     * @return the character which this escape sequence represents.
     */
    private static char convertEscSeqChar(char c) {
        switch (c) {
            case '0': return '\0';
            case 'b': return '\b';
            case 'f': return '\f';
            case 'n': return '\n';
            case 'r': return '\r';
            case 't': return '\t';
            case 'Z': return '\032';

            default:  return c;
        }
    }
}
