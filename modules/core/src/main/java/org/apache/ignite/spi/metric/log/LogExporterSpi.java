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

package org.apache.ignite.spi.metric.log;

import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.spi.metric.list.SystemView;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.spi.metric.list.SystemViewRowAttributeWalker;
import org.apache.ignite.spi.metric.list.SystemViewRowAttributeWalker.AttributeVisitor;
import org.apache.ignite.spi.metric.list.SystemViewRowAttributeWalker.AttributeWithValueVisitor;

/**
 * This SPI implementation exports metrics to Ignite log.
 */
public class LogExporterSpi extends PushMetricsExporterAdapter {
    /** Column separator. */
    public static final char COL_SEPARATOR = ',';

    /** {@inheritDoc} */
    @Override public void export() {
        if (!log.isInfoEnabled()) {
            LT.warn(log, "LogExporterSpi configured but INFO level is disabled. " +
                "Metrics will not be export to the log! Please, check logger settings.");

            return;
        }

        log.info("Metrics:");

        mreg.forEach(grp -> {
            if (mregFilter != null && !mregFilter.test(grp))
                return;

            grp.forEach(m -> log.info(m.name() + " = " + m.getAsString()));
        });

        log.info("Lists:");

        mlreg.forEach(this::exportList);
    }

    /**
     * Prints list data in CSV style format.
     *
     * @param list List to print.
     * @param <R> Row type.
     */
    private <R> void exportList(SystemView<R> list) {
        if (sviewFilter != null && !sviewFilter.test(list))
            return;

        log.info(list.name());

        StringBuilder names = new StringBuilder();

        SystemViewRowAttributeWalker<R> walker = list.walker();

        walker.visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                if (idx != 0)
                    names.append(COL_SEPARATOR);

                names.append(name);
            }
        });

        log.info(names.toString());

        for (R row : list) {
            StringBuilder rowStr = new StringBuilder();

            walker.visitAll(row, new AttributeWithValueVisitor() {
                @Override public <T> void accept(int idx, String name, Class<T> clazz, T val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }

                @Override public void acceptBoolean(int idx, String name, boolean val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }

                @Override public void acceptChar(int idx, String name, char val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }

                @Override public void acceptByte(int idx, String name, byte val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }

                @Override public void acceptShort(int idx, String name, short val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }

                @Override public void acceptInt(int idx, String name, int val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }

                @Override public void acceptLong(int idx, String name, long val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }

                @Override public void acceptFloat(int idx, String name, float val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }

                @Override public void acceptDouble(int idx, String name, double val) {
                    if (idx != 0)
                        rowStr.append(COL_SEPARATOR);

                    rowStr.append(val);
                }
            });

            log.info(rowStr.toString());
        }
    }
}
