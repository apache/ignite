package org.apache.ignite.internal.sql;

import org.apache.ignite.IgniteIllegalStateException;

/** FIXME */
public class SqlEscSeqParser {

    public static final int SINGLE_CHAR_RADIX = -1;

    /** FIXME */
    public enum Mode {
        /** FIXME */
        START,
        /** FIXME */
        PROCESSING,
        /** FIXME */
        FINISHED_ACCEPTED,
        /** FIXME */
        FINISHED_REJECTED,
        /** FIXME */
        ERROR
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
    private final StringBuffer input = new StringBuffer();

    /** FIXME */
    public SqlEscSeqParser() {
        mode = Mode.START;
    }

    /** FIXME */
    public Mode accept(char c) {

        if(mode == Mode.START)
            acceptPrefix(c);
        else
            acceptValueChar(c);

        return mode;
    }

    /** FIXME */
    private void acceptPrefix(char c) {
        if (mode != Mode.START)
            throw new IgniteIllegalStateException("Internal error");

        mode = Mode.PROCESSING;

        switch (c) {
            case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7':
                minLen = 1;
                maxLen = 3;
                radix = 8;
                acceptValueChar(c);
                break;

            case 'x': case 'X':
                minLen = 1;
                maxLen = 2;
                radix = 16;
                break;

            // Upper-case 'U' is intentionally not supported (as in Postgres),
            // because in Java we don't support anything beyond Basic Multilingual Plane
            // because characters above it (with codepoints >= 0x10000, supplementary ones, surrogates)
            // are complex to handle. We still allow the surrogates.
            case 'u':
                minLen = 4;
                maxLen = 4;
                radix = 16;
                break;

            default:
                minLen = 1;
                maxLen = 1;
                radix = SINGLE_CHAR_RADIX;
                acceptValueChar(c);
        }
    }

    /** FIXME */
    private void acceptValueChar(char c) {
        int inputLen = input.length();

        if (mode == Mode.PROCESSING) {

            if (radix != SINGLE_CHAR_RADIX && !isValidDigit(c)) {

                if (inputLen >= minLen && isValidUnicodeInput())
                    mode = Mode.FINISHED_REJECTED;
                else
                    mode = Mode.ERROR;

                return;
            }
        }

        if (inputLen >= maxLen || !isValidInput(c)) {
            if (isValidUnicodeInput())
                mode = Mode.FINISHED_REJECTED;
            else
                mode = Mode.ERROR;

            return;
        }

        input.append(c);

        if (input.length() >= maxLen) {

            if (isValidUnicodeInput())
                mode = Mode.FINISHED_ACCEPTED;
            else
                mode = Mode.ERROR;
        }
    }

    /** FIXME */
    private boolean isValidDigit(char c) {
        if (radix < 10)
            return c >= '0' && c < ('0' + radix);
        else
            return (c >= '0' && c <= '9') ||
                (c >= 'a' && c < 'a' + (radix - 10)) ||
                (c >= 'A' && c < 'A' + (radix - 10));
    }

    /** FIXME */
    private boolean isValidInput(char c) {
        if (radix == 8 && input.length() == 2 && input.charAt(0) >= '4')
            return false;

        return true;
    }

    private boolean isValidUnicodeInput() {
        if (radix != 16)
            return true;

        int codePnt = Integer.parseInt(input.toString(), radix);

        return Character.isValidCodePoint(codePnt);
    }

    /** FIXME */
    public String convertedStr() {
        if (mode != Mode.FINISHED_ACCEPTED && mode != Mode.FINISHED_REJECTED)
            throw new IgniteIllegalStateException("Internal error");

        if (radix == SINGLE_CHAR_RADIX)
            return Character.toString(convertEscSeqChar(input.charAt(0)));
        else
            return new String(Character.toChars(Integer.parseInt(input.toString(), radix)));
    }

    /**
     * Converts second character from escape sequence to actual character
     *
     * @param c The character after the backquote.
     * @return the character which this escape sequence represents.
     */
    private static char convertEscSeqChar(char c) {
        switch (c) {
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
