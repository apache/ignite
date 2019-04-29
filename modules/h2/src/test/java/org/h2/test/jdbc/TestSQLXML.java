/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.IOUtils;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Test the SQLXML implementation.
 */
public class TestSQLXML extends TestDb {
    private static final String XML = "<xml a=\"v\">Text</xml>";

    private JdbcConnection conn;
    private Statement stat;

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb(getTestName());
        conn = (JdbcConnection) getConnection(getTestName());
        stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, X CLOB)");
        stat.execute("INSERT INTO TEST VALUES (1, NULL)");
        testGetters();
        testSetters();
        conn.close();
        deleteDb(getTestName());
    }

    private void testGetters() throws SQLException, IOException, XMLStreamException {
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        assertNull(rs.getSQLXML(2));
        assertNull(rs.getSQLXML("X"));
        assertEquals(1, stat.executeUpdate("UPDATE TEST SET X = '" + XML + '\''));
        rs = stat.executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        // ResultSet.getObject()
        SQLXML sqlxml = rs.getObject(2, SQLXML.class);
        assertEquals(XML, sqlxml.getString());

        sqlxml = rs.getSQLXML(2);
        // getBinaryStream()
        assertEquals(XML, IOUtils.readStringAndClose(IOUtils.getReader(sqlxml.getBinaryStream()), -1));
        // getCharacterStream()
        assertEquals(XML, IOUtils.readStringAndClose(sqlxml.getCharacterStream(), -1));
        // getString()
        assertEquals(XML, sqlxml.getString());
        // DOMSource
        DOMSource domSource = sqlxml.getSource(DOMSource.class);
        Node n = domSource.getNode().getFirstChild();
        assertEquals("xml", n.getNodeName());
        assertEquals("v", n.getAttributes().getNamedItem("a").getNodeValue());
        assertEquals("Text", n.getFirstChild().getNodeValue());
        // SAXSource
        SAXSource saxSource = sqlxml.getSource(SAXSource.class);
        assertEquals(XML,
                IOUtils.readStringAndClose(IOUtils.getReader(saxSource.getInputSource().getByteStream()), -1));
        // StAXSource
        StAXSource staxSource = sqlxml.getSource(StAXSource.class);
        XMLStreamReader stxReader = staxSource.getXMLStreamReader();
        assertEquals(XMLStreamReader.START_DOCUMENT, stxReader.getEventType());
        assertEquals(XMLStreamReader.START_ELEMENT, stxReader.next());
        assertEquals("xml", stxReader.getLocalName());
        assertEquals("a", stxReader.getAttributeLocalName(0));
        assertEquals("v", stxReader.getAttributeValue(0));
        assertEquals(XMLStreamReader.CHARACTERS, stxReader.next());
        assertEquals("Text", stxReader.getText());
        assertEquals(XMLStreamReader.END_ELEMENT, stxReader.next());
        assertEquals(XMLStreamReader.END_DOCUMENT, stxReader.next());
        // StreamSource
        StreamSource streamSource = sqlxml.getSource(StreamSource.class);
        assertEquals(XML, IOUtils.readStringAndClose(IOUtils.getReader(streamSource.getInputStream()), -1));
        // something illegal
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, sqlxml).getSource(Source.class);
    }

    private void testSetters() throws SQLException, IOException, SAXException, ParserConfigurationException,
            TransformerConfigurationException, TransformerException {
        // setBinaryStream()
        SQLXML sqlxml = conn.createSQLXML();
        try (OutputStream out = sqlxml.setBinaryStream()) {
            out.write(XML.getBytes(StandardCharsets.UTF_8));
        }
        testSettersImpl(sqlxml);
        // setCharacterStream()
        sqlxml = conn.createSQLXML();
        try (Writer out = sqlxml.setCharacterStream()) {
            out.write(XML);
        }
        testSettersImpl(sqlxml);
        // setString()
        sqlxml = conn.createSQLXML();
        sqlxml.setString(XML);
        testSettersImpl(sqlxml);

        TransformerFactory tf = TransformerFactory.newInstance();
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DOMSource domSource = new DOMSource(dbf.newDocumentBuilder().parse(new InputSource(new StringReader(XML))));
        // DOMResult
        sqlxml = conn.createSQLXML();
        tf.newTransformer().transform(domSource, sqlxml.setResult(DOMResult.class));
        testSettersImpl(sqlxml);
        // SAXResult
        sqlxml = conn.createSQLXML();
        tf.newTransformer().transform(domSource, sqlxml.setResult(SAXResult.class));
        testSettersImpl(sqlxml);
        // StAXResult
        sqlxml = conn.createSQLXML();
        tf.newTransformer().transform(domSource, sqlxml.setResult(StAXResult.class));
        testSettersImpl(sqlxml);
        // StreamResult
        sqlxml = conn.createSQLXML();
        tf.newTransformer().transform(domSource, sqlxml.setResult(StreamResult.class));
        testSettersImpl(sqlxml);
        // something illegal
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, sqlxml).setResult(Result.class);
        // null
        testSettersImpl(null);
    }

    private void assertXML(String actual) {
        if (actual.startsWith("<?")) {
            actual = actual.substring(actual.indexOf("?>") + 2);
        }
        assertEquals(XML, actual);
    }

    private void testSettersImplAssert(SQLXML sqlxml) throws SQLException {
        ResultSet rs = stat.executeQuery("SELECT X FROM TEST");
        assertTrue(rs.next());
        SQLXML v = rs.getSQLXML(1);
        if (sqlxml != null) {
            assertXML(v.getString());
        } else {
            assertNull(v);
        }
    }

    private void testSettersImpl(SQLXML sqlxml) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("UPDATE TEST SET X = ?");
        prep.setSQLXML(1, sqlxml);
        assertEquals(1, prep.executeUpdate());
        testSettersImplAssert(sqlxml);

        prep.setObject(1, sqlxml);
        assertEquals(1, prep.executeUpdate());
        testSettersImplAssert(sqlxml);

        Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = st.executeQuery("SELECT * FROM TEST FOR UPDATE");
        assertTrue(rs.next());
        rs.updateSQLXML(2, sqlxml);
        rs.updateRow();
        testSettersImplAssert(sqlxml);
        rs = st.executeQuery("SELECT * FROM TEST FOR UPDATE");
        assertTrue(rs.next());
        rs.updateSQLXML("X", sqlxml);
        rs.updateRow();
        testSettersImplAssert(sqlxml);

        rs = st.executeQuery("SELECT * FROM TEST FOR UPDATE");
        assertTrue(rs.next());
        rs.updateObject(2, sqlxml);
        rs.updateRow();
        testSettersImplAssert(sqlxml);
    }

}
