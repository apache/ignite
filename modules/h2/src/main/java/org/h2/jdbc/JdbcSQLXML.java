/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.value.Value;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

/**
 * Represents a SQLXML value.
 */
public class JdbcSQLXML extends JdbcLob implements SQLXML {

    private DOMResult domResult;

    /**
     * Underlying stream for SAXResult, StAXResult, and StreamResult.
     */
    private Closeable closable;

    /**
     * INTERNAL
     */
    public JdbcSQLXML(JdbcConnection conn, Value value, State state, int id) {
        super(conn, value, state, TraceObject.SQLXML, id);
    }

    @Override
    void checkReadable() throws SQLException, IOException {
        checkClosed();
        if (state == State.SET_CALLED) {
            if (domResult != null) {
                Node node = domResult.getNode();
                domResult = null;
                TransformerFactory factory = TransformerFactory.newInstance();
                try {
                    Transformer transformer = factory.newTransformer();
                    DOMSource domSource = new DOMSource(node);
                    StringWriter stringWriter = new StringWriter();
                    StreamResult streamResult = new StreamResult(stringWriter);
                    transformer.transform(domSource, streamResult);
                    completeWrite(conn.createClob(new StringReader(stringWriter.toString()), -1));
                } catch (Exception e) {
                    throw logAndConvert(e);
                }
                return;
            } else if (closable != null) {
                closable.close();
                closable = null;
                return;
            }
            throw DbException.getUnsupportedException("Stream setter is not yet closed.");
        }
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return super.getBinaryStream();
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return super.getCharacterStream();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall(
                        "getSource(" + (sourceClass != null ? sourceClass.getSimpleName() + ".class" : "null") + ')');
            }
            checkReadable();
            if (sourceClass == null || sourceClass == DOMSource.class) {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                return (T) new DOMSource(dbf.newDocumentBuilder().parse(new InputSource(value.getInputStream())));
            } else if (sourceClass == SAXSource.class) {
                return (T) new SAXSource(new InputSource(value.getInputStream()));
            } else if (sourceClass == StAXSource.class) {
                XMLInputFactory xif = XMLInputFactory.newInstance();
                return (T) new StAXSource(xif.createXMLStreamReader(value.getInputStream()));
            } else if (sourceClass == StreamSource.class) {
                return (T) new StreamSource(value.getInputStream());
            }
            throw unsupported(sourceClass.getName());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public String getString() throws SQLException {
        try {
            debugCodeCall("getString");
            checkReadable();
            return value.getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
        try {
            debugCodeCall("setBinaryStream");
            checkEditable();
            state = State.SET_CALLED;
            return new BufferedOutputStream(setClobOutputStreamImpl());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
        try {
            debugCodeCall("setCharacterStream");
            checkEditable();
            state = State.SET_CALLED;
            return setCharacterStreamImpl();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall(
                        "getSource(" + (resultClass != null ? resultClass.getSimpleName() + ".class" : "null") + ')');
            }
            checkEditable();
            if (resultClass == null || resultClass == DOMResult.class) {
                domResult = new DOMResult();
                state = State.SET_CALLED;
                return (T) domResult;
            } else if (resultClass == SAXResult.class) {
                SAXTransformerFactory transformerFactory = (SAXTransformerFactory) TransformerFactory.newInstance();
                TransformerHandler transformerHandler = transformerFactory.newTransformerHandler();
                Writer writer = setCharacterStreamImpl();
                transformerHandler.setResult(new StreamResult(writer));
                SAXResult saxResult = new SAXResult(transformerHandler);
                closable = writer;
                state = State.SET_CALLED;
                return (T) saxResult;
            } else if (resultClass == StAXResult.class) {
                XMLOutputFactory xof = XMLOutputFactory.newInstance();
                Writer writer = setCharacterStreamImpl();
                StAXResult staxResult = new StAXResult(xof.createXMLStreamWriter(writer));
                closable = writer;
                state = State.SET_CALLED;
                return (T) staxResult;
            } else if (StreamResult.class.equals(resultClass)) {
                Writer writer = setCharacterStreamImpl();
                StreamResult streamResult = new StreamResult(writer);
                closable = writer;
                state = State.SET_CALLED;
                return (T) streamResult;
            }
            throw unsupported(resultClass.getName());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public void setString(String value) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall("getSource", value);
            }
            checkEditable();
            completeWrite(conn.createClob(new StringReader(value), -1));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

}
