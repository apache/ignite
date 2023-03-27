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

package org.apache.ignite.testframework;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * A Log4j2 {@link org.apache.logging.log4j.core.appender.AbstractAppender} that memorizes all the events it gets from loggers.
 * These events are made available to the class users.
 */
public class MemorizingAppender extends AbstractAppender {
    /**
     * Events that were seen by this Appender.
     */
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override public void append(LogEvent event) {
        events.add(event);
    }

    /** */
    public MemorizingAppender() {
        super(MemorizingAppender.class.getName(), null, null, true, Property.EMPTY_ARRAY);
    }

    /**
     * Returns all events that were seen by this Appender so far.
     *
     * @return All events that were seen by this Appender so far.
     */
    public List<LogEvent> events() {
        return new ArrayList<>(events);
    }

    /**
     * Adds this Appender to the logger corresponding to the provided class.
     *
     * @param target Class on whose logger to install this Appender.
     */
    public void installSelfOn(Class<?> target) {
        LoggerContext ctx = LoggerContext.getContext(false);

        Configuration cfg = ctx.getConfiguration();

        LoggerConfig logCfg = cfg.getLoggers().get(target.getName());

        if (logCfg == null) {
            logCfg = new LoggerConfig(target.getName(), cfg.getLoggerConfig(target.getName()).getLevel(), true);

            cfg.addLogger(target.getName(), logCfg);
        }

        logCfg.addAppender(this, null, null);

        ctx.updateLoggers();
    }

    /**
     * Removes this Appender from the logger corresponding to the provided class.
     *
     * @param target Class from whose logger to remove this Appender.
     */
    public void removeSelfFrom(Class<?> target) {
        LoggerConfig logCfg = LoggerContext.getContext(false).getConfiguration().getLoggerConfig(target.getName());

        logCfg.removeAppender(getName());

        LoggerContext.getContext(false).updateLoggers();
    }

    /**
     * Returns the single event satisfying the given predicate. If no such event exists or more than one event matches,
     * then an exception is thrown.
     *
     * @param predicate Predicate to use to select the event.
     * @return The single event satisfying the given predicate.
     */
    public LogEvent singleEventSatisfying(Predicate<LogEvent> predicate) {
        List<LogEvent> matches = events.stream().filter(predicate).collect(toList());

        assertThat(matches, hasSize(1));

        return matches.get(0);
    }
}
