/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.util.Arrays;

/**
 * This class implements a simple XML pull parser.
 * Only a subset of the XML pull parser API is implemented.
 */
public class XMLParser {

    /**
     * This event type means an error occurred.
     */
    public static final int ERROR = 0;

    /**
     * This event type means a start element has been read.
     */
    public static final int START_ELEMENT = 1;

    /**
     * This event type means an end element has been read.
     */
    public static final int END_ELEMENT = 2;

    /**
     * This event type means a processing instruction has been read.
     */
    public static final int PROCESSING_INSTRUCTION = 3;

    /**
     * This event type means text has been read.
     */
    public static final int CHARACTERS = 4;

    /**
     * This event type means a comment has been read.
     */
    public static final int COMMENT = 5;

    // public static final int SPACE = 6;

    /**
     * This event type is used before reading.
     */
    public static final int START_DOCUMENT = 7;

    /**
     * This event type means the end of the document has been reached.
     */
    public static final int END_DOCUMENT = 8;

    // public static final int ENTITY_REFERENCE = 9;
    // public static final int ATTRIBUTE = 10;

    /**
     * This event type means a DTD element has been read.
     */
    public static final int DTD = 11;

    private final String xml;
    private int pos;
    private int eventType;
    private String currentText;
    private String currentToken;
    private String prefix, localName;
    private String[] attributeValues = new String[3];
    private int currentAttribute;
    private boolean endElement;
    private boolean html;

    /**
     * Construct a new XML parser.
     *
     * @param xml the document
     */
    public XMLParser(String xml) {
        this.xml = xml;
        eventType = START_DOCUMENT;
    }

    /**
     * Enable or disable HTML processing. When enabled, attributes don't need to
     * have values.
     *
     * @param html true if HTML processing is enabled.
     */
    public void setHTML(boolean html) {
        this.html = html;
    }

    private void addAttributeName(String pre, String name) {
        if (attributeValues.length <= currentAttribute) {
            attributeValues = Arrays.copyOf(attributeValues, attributeValues.length * 2);
        }
        attributeValues[currentAttribute++] = pre;
        attributeValues[currentAttribute++] = name;
    }

    private void addAttributeValue(String v) {
        attributeValues[currentAttribute++] = v;
    }

    private int readChar() {
        if (pos >= xml.length()) {
            return -1;
        }
        return xml.charAt(pos++);
    }

    private void back() {
        pos--;
    }

    private void error(String expected) {
        throw new RuntimeException("Expected: " + expected + " got: "
                + xml.substring(pos, Math.min(pos + 1000, xml.length())));
    }

    private void read(String chars) {
        for (int i = 0; i < chars.length(); i++) {
            if (readChar() != chars.charAt(i)) {
                error(chars);
            }
        }
    }

    private void skipSpaces() {
        while (pos < xml.length() && xml.charAt(pos) <= ' ') {
            pos++;
        }
    }

    private void read() {
        currentText = null;
        currentAttribute = 0;
        int tokenStart = pos, currentStart = pos;
        int ch = readChar();
        if (ch < 0) {
            eventType = END_DOCUMENT;
        } else if (ch == '<') {
            currentStart = pos;
            ch = readChar();
            if (ch < 0) {
                eventType = ERROR;
            } else if (ch == '?') {
                eventType = PROCESSING_INSTRUCTION;
                currentStart = pos;
                while (true) {
                    ch = readChar();
                    if (ch < 0) {
                        error("?>");
                    }
                    if (ch == '?' && readChar() == '>') {
                        break;
                    }
                }
                if (xml.substring(currentStart).startsWith("xml")) {
                    int back = tokenStart;
                    read();
                    tokenStart = back;
                } else {
                    currentText = xml.substring(currentStart, pos - 1);
                }
            } else if (ch == '!') {
                ch = readChar();
                if (ch == '-') {
                    eventType = COMMENT;
                    if (readChar() != '-') {
                        error("-");
                    }
                    currentStart = pos;
                    while (true) {
                        ch = readChar();
                        if (ch < 0) {
                            error("-->");
                        }
                        if (ch == '-' && readChar() == '-') {
                            read(">");
                            break;
                        }
                    }
                    currentText = xml.substring(currentStart, pos - 1);
                } else if (ch == 'D') {
                    read("OCTYPE");
                    eventType = DTD;
                    while (true) {
                        ch = readChar();
                        if (ch < 0) {
                            break;
                        }
                        if (ch == '>') {
                            break;
                        }
                    }
                } else if (ch == '[') {
                    read("CDATA[");
                    currentStart = pos;
                    eventType = CHARACTERS;
                    while (true) {
                        ch = readChar();
                        if (ch < 0) {
                            error("]]>");
                        } else if (ch != ']') {
                            continue;
                        }
                        ch = readChar();
                        if (ch < 0) {
                            error("]]>");
                        } else if (ch == ']') {
                            do {
                                ch = readChar();
                                if (ch < 0) {
                                    error("]]>");
                                }
                            } while (ch == ']');
                            if (ch == '>') {
                                currentText = xml.substring(currentStart, pos - 3);
                                break;
                            }
                        }
                    }
                }
            } else if (ch == '/') {
                currentStart = pos;
                prefix = null;
                eventType = END_ELEMENT;
                while (true) {
                    ch = readChar();
                    if (ch < 0) {
                        error(">");
                    } else if (ch == ':') {
                        prefix = xml.substring(currentStart, pos - 1);
                        currentStart = pos + 1;
                    } else if (ch == '>') {
                        localName = xml.substring(currentStart, pos - 1);
                        break;
                    } else if (ch <= ' ') {
                        localName = xml.substring(currentStart, pos - 1);
                        skipSpaces();
                        read(">");
                        break;
                    }
                }
            } else {
                prefix = null;
                localName = null;
                eventType = START_ELEMENT;
                while (true) {
                    ch = readChar();
                    if (ch < 0) {
                        error(">");
                    } else if (ch == ':') {
                        prefix = xml.substring(currentStart, pos - 1);
                        currentStart = pos + 1;
                    } else if (ch <= ' ') {
                        localName = xml.substring(currentStart, pos - 1);
                        readAttributeValues();
                        ch = readChar();
                    }
                    if (ch == '/') {
                        if (localName == null) {
                            localName = xml.substring(currentStart, pos - 1);
                        }
                        read(">");
                        endElement = true;
                        break;
                    } else if (ch == '>') {
                        if (localName == null) {
                            localName = xml.substring(currentStart, pos - 1);
                        }
                        break;
                    }
                }
            }
        } else {
            // TODO need to replace &#xx;?
            eventType = CHARACTERS;
            while (true) {
                ch = readChar();
                if (ch < 0) {
                    break;
                } else if (ch == '<') {
                    back();
                    break;
                }
            }
            currentText = xml.substring(currentStart, pos);
        }
        currentToken = xml.substring(tokenStart, pos);
    }

    private void readAttributeValues() {
        while (true) {
            int start = pos;
            int ch = readChar();
            if (ch < 0) {
                error(">");
            } else if (ch <= ' ') {
                continue;
            } else if (ch == '/' || ch == '>') {
                back();
                return;
            }
            int end;
            int localNameStart = start;
            boolean noValue = false;
            while (true) {
                end = pos;
                ch = readChar();
                if (ch < 0) {
                    error("=");
                } else if (ch <= ' ') {
                    skipSpaces();
                    ch = readChar();
                    if (ch != '=') {
                        if (html) {
                            back();
                            noValue = true;
                        } else {
                            error("=");
                        }
                    }
                    break;
                } else if (ch == '=') {
                    break;
                } else if (ch == ':') {
                    localNameStart = pos;
                } else if (ch == '/' || ch == '>') {
                    if (html) {
                        back();
                        noValue = true;
                        break;
                    }
                    error("=");
                }
            }
            if (localNameStart == start) {
                addAttributeName("", xml.substring(localNameStart, end));
            } else {
                addAttributeName(xml.substring(start, localNameStart - 1),
                        xml.substring(localNameStart, end));
            }
            if (noValue) {
                noValue = false;
            } else {
                skipSpaces();
                ch = readChar();
                if (ch != '\"') {
                    error("\"");
                }
                start = pos;
                while (true) {
                    end = pos;
                    ch = readChar();
                    if (ch < 0) {
                        error("\"");
                    } else if (ch == '\"') {
                        break;
                    }
                }
            }
            addAttributeValue(xml.substring(start, end));
        }
    }

    /**
     * Check if there are more tags to read.
     *
     * @return true if there are more tags
     */
    public boolean hasNext() {
        return pos < xml.length();
    }

    /**
     * Read the next tag.
     *
     * @return the event type of the next tag
     */
    public int next() {
        if (endElement) {
            endElement = false;
            eventType = END_ELEMENT;
            currentToken = "";
        } else {
            read();
        }
        return eventType;
    }

    /**
     * Get the event type of the current token.
     *
     * @return the event type
     */
    public int getEventType() {
        return eventType;
    }

    /**
     * Get the current text.
     *
     * @return the text
     */
    public String getText() {
        return currentText;
    }

    /**
     * Get the current token text.
     *
     * @return the token
     */
    public String getToken() {
        return currentToken;
    }

    /**
     * Get the number of attributes.
     *
     * @return the attribute count
     */
    public int getAttributeCount() {
        return currentAttribute / 3;
    }

    /**
     * Get the prefix of the attribute.
     *
     * @param index the index of the attribute (starting with 0)
     * @return the prefix
     */
    public String getAttributePrefix(int index) {
        return attributeValues[index * 3];
    }

    /**
     * Get the local name of the attribute.
     *
     * @param index the index of the attribute (starting with 0)
     * @return the local name
     */
    public String getAttributeLocalName(int index) {
        return attributeValues[index * 3 + 1];
    }

    /**
     * Get the full name of the current start or end element. If there is no
     * prefix, only the local name is returned, otherwise the prefix, ':', and
     * the local name.
     *
     * @return the full name
     */
    public String getName() {
        return prefix == null || prefix.length() == 0 ? localName : prefix
                + ":" + localName;
    }

    /**
     * Get the local name of the current start or end element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return localName;
    }

    /**
     * Get the prefix of the current start or end element.
     *
     * @return the prefix
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * Check if the current character tag only contains spaces or other
     * non-printable characters.
     *
     * @return if the trimmed text is empty
     */
    public boolean isWhiteSpace() {
        return getText().trim().length() == 0;
    }

    /**
     * Get the remaining XML text of the document.
     *
     * @return the remaining XML
     */
    public String getRemaining() {
        return xml.substring(pos);
    }

    /**
     * Get the current character position in the XML document.
     *
     * @return the position
     */
    public int getPos() {
        return pos;
    }

}
