/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tools.junit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.junit.Ignore;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * A JUnit RunListener that produces output conforming to the Teamcity messages specification.
 *
 * Inspired by https://github.com/mbruggmann/junit-teamcity-reporter/
 */
public class JUnitTeamcityReporter extends RunListener {
    /** */
    private static final long FLUSH_THRESHOLD = 5 * 60 * 1000;

    /** */
    public static volatile String suite;

    /** */
    private final Path reportDir;

    /** */
    private final XMLOutputFactory outputFactory;

    /** */
    private String prevSuite;

    /** */
    private String prevTestCls;

    /** */
    private long prevFlush;

    /** */
    private FileOutputStream curStream;

    /** */
    private XMLStreamWriter curXmlStream;

    /** */
    public JUnitTeamcityReporter() throws IOException {
        reportDir = Files.createTempDirectory("ignite-tools-junit-reports");
        outputFactory = XMLOutputFactory.newInstance();
    }

    /** */
    @Override public synchronized void testAssumptionFailure(Failure failure) {
        if (curXmlStream == null)
            testStarted(failure.getDescription());

        try {
            curXmlStream.writeStartElement("skipped");

            if (failure.getMessage() != null)
                curXmlStream.writeAttribute("message", failure.getMessage());

            curXmlStream.writeEndElement();
        }
        catch (XMLStreamException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** */
    @Override public synchronized void testStarted(Description desc) {
        if (!desc.getClassName().equals(prevTestCls))
            System.out.println(String.format("##teamcity[progressMessage 'Running %s']",
                escapeForTeamcity(desc.getClassName())));

        try {
            if (afterFlush(desc.getClassName())) {
                prevSuite = suite;
                prevFlush = System.currentTimeMillis();

                curStream = new FileOutputStream(reportDir.resolve(fileName()).toFile());

                curXmlStream = outputFactory.createXMLStreamWriter(curStream);

                curXmlStream.writeStartDocument();
                curXmlStream.writeStartElement("testsuite");
                curXmlStream.writeAttribute("version", "3.0");
                curXmlStream.writeAttribute("name", suite != null ? suite : desc.getClassName());
            }

            prevTestCls = desc.getClassName();

            curXmlStream.writeStartElement("testcase");
            curXmlStream.writeAttribute("name", desc.getMethodName() != null ? desc.getMethodName() : "");
            curXmlStream.writeAttribute("classname", desc.getClassName());

            // Avoid doubling of run time after the surefire-generated full report is ingested:
            curXmlStream.writeAttribute("time", "0");
        }
        catch (XMLStreamException | FileNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** */
    @Override public synchronized void testFinished(Description desc) {
        if (curXmlStream == null)
            testStarted(desc);

        try {
            curXmlStream.writeEndElement();
        }
        catch (XMLStreamException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** */
    @Override public synchronized void testFailure(Failure failure) {
        if (curXmlStream == null)
            testStarted(failure.getDescription());

        try {
            curXmlStream.writeStartElement("failure");

            if (failure.getException() != null && failure.getException().getMessage() != null)
                curXmlStream.writeAttribute("type", failure.getException().getMessage());

            if (failure.getMessage() != null)
                curXmlStream.writeCData(failure.getMessage());

            curXmlStream.writeEndElement();

            if (failure.getException() != null) {
                curXmlStream.writeStartElement("system-out");
                curXmlStream.writeCData(X.getFullStackTrace(failure.getException()));
                curXmlStream.writeEndElement();
            }
        }
        catch (XMLStreamException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** */
    @Override public synchronized void testIgnored(Description desc) {
        testStarted(desc);

        Ignore annotation = desc.getAnnotation(Ignore.class);

        try {
            curXmlStream.writeStartElement("skipped");

            if (annotation != null)
                curXmlStream.writeAttribute("message", annotation.value());

            curXmlStream.writeEndElement();
        }
        catch (XMLStreamException ex) {
            throw new RuntimeException(ex);
        }

        testFinished(desc);
    }

    /** */
    private boolean afterFlush(String testCls) {
        if (curStream == null)
            return true;

        if ((prevSuite == null ? suite != null : !prevSuite.equals(suite)) ||
            (prevTestCls == null ? testCls != null : !prevTestCls.equals(testCls)) ||
            (System.currentTimeMillis() - prevFlush) > FLUSH_THRESHOLD) {
            try {
                curXmlStream.writeEndElement();
                curXmlStream.writeEndDocument();
                curXmlStream.close();
                curStream.close();
            }
            catch (XMLStreamException | IOException ex) {
                throw new RuntimeException(ex);
            }

            File report = reportDir.resolve(fileName()).toFile();

            assert report.exists();

            System.out.println(String.format("##teamcity[importData type='surefire' path='%s']",
                escapeForTeamcity(report.getAbsolutePath())));

            return true;
        }

        return false;
    }

    /** */
    private String fileName() {
        return "test-" + prevSuite + prevFlush + ".xml";
    }

    /** */
    private String escapeForTeamcity(String msg) {
        return (msg == null ? "null" : msg)
            .replace("|", "||")
            .replace("\r", "|r")
            .replace("\n", "|n")
            .replace("'", "|'")
            .replace("[", "|[")
            .replace("]", "|]");
    }
}

