/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.schedule;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.util.StringUtils;

/**
 * Cron Expression
 */
public class CronExpression {

    /** Cron. */
    private final String cron;

    /**
     * @param cron Cron.
     */
    public CronExpression(String cron) throws IgniteCheckedException {
        try {
            A.notNullOrEmpty(cron, "cron");

            this.cron = appendDayOfWeekIfNeeded(cron.trim());

            new CronSequenceGenerator(this.cron);
        }
        catch (IllegalArgumentException | NullPointerException e) {
            throw new IgniteCheckedException("Invalid cron expression in schedule pattern: " + cron, e);
        }
    }

    /**
     * @param cron pattern
     * @return if day of week is omitted in the pattern adds "?" to satisfy {@link CronSequenceGenerator} requirements
     */
    private static String appendDayOfWeekIfNeeded(String cron) {
        String[] fields = StringUtils.tokenizeToStringArray(cron, " ");

        if (fields != null && fields.length == 5)
            return cron + " ?"; // add unspecified Day-of-Week
        else
            return cron;
    }

    /**
     * @return Cron
     */
    public String getCron() {
        return cron;
    }

}
