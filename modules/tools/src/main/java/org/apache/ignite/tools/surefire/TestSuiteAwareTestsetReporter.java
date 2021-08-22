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

package org.apache.ignite.tools.surefire;

import org.apache.ignite.tools.junit.JUnitTeamcityReporter;
import org.apache.maven.plugin.surefire.extensions.SurefireStatelessTestsetInfoReporter;
import org.apache.maven.plugin.surefire.log.api.ConsoleLogger;
import org.apache.maven.plugin.surefire.report.ConsoleReporter;
import org.apache.maven.plugin.surefire.report.TestSetStats;
import org.apache.maven.plugin.surefire.report.WrappedReportEntry;
import org.apache.maven.surefire.extensions.StatelessTestsetInfoConsoleReportEventListener;
import org.apache.maven.surefire.report.ReportEntry;
import org.apache.maven.surefire.report.TestSetReportEntry;
import org.apache.maven.surefire.shade.common.org.apache.maven.shared.utils.logging.MessageBuilder;
import org.apache.maven.surefire.shade.common.org.apache.maven.shared.utils.logging.MessageUtils;

import static org.apache.maven.surefire.report.CategorizedReportEntry.GROUP_PREFIX;

/** */
public class TestSuiteAwareTestsetReporter extends SurefireStatelessTestsetInfoReporter {
    /** */
    @Override public StatelessTestsetInfoConsoleReportEventListener<WrappedReportEntry, TestSetStats> createListener(
        ConsoleLogger log) {
        return new ConsoleReporter(log, false, false) {
            /** */
            public void testSetStarting(TestSetReportEntry report) {
                MessageBuilder builder = MessageUtils.buffer();

                JUnitTeamcityReporter.suite = concatenateWithTestGroup(builder, report);

                super.testSetStarting(report);
            }
        };
    }

    /**
     * @see TestSetStats#concatenateWithTestGroup(MessageBuilder, ReportEntry, boolean)
     */
    private static String concatenateWithTestGroup(MessageBuilder builder, ReportEntry report)
    {
        String testCls = report.getNameWithGroup();

        int idxOfGrp = testCls.indexOf(GROUP_PREFIX);

        int delim = testCls.lastIndexOf('.', idxOfGrp == -1 ? testCls.length() : idxOfGrp);

        String pkg = testCls.substring(0, 1 + delim);
        String cls = testCls.substring(1 + delim);

        return builder.a(pkg).a(cls).toString();
    }
}
