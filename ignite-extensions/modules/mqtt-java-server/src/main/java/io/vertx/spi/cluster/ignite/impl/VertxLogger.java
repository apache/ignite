/*
 * Copyright 2020 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.vertx.spi.cluster.ignite.impl;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * @author Lukas Prettenthaler
 */
public class VertxLogger implements IgniteLogger {
  private static final Logger logger = LoggerFactory.getLogger(IgniteLogger.class);

  @Override
  public IgniteLogger getLogger(Object ctgr) {
    return this;
  }

  @Override
  public void trace(String msg) {
    logger.trace(msg);
  }

  @Override
  public void debug(String msg) {
    logger.debug(msg);
  }

  @Override
  public void info(String msg) {
    logger.info(msg);
  }

  @Override
  public void warning(String msg, @Nullable Throwable e) {
    logger.warn(msg, e);
  }

  @Override
  public void error(String msg, @Nullable Throwable e) {
    logger.error(msg, e);
  }

  @Override
  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public boolean isQuiet() {
    return false;
  }

  @Override
  public String fileName() {
    return null;
  }
}
