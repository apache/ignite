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

package org.apache.ignite.ml.environment.logging.formatter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.ml.Model;

/**
 * Encapsulates collection of Vector and Model MLFormatters.
 */
public class MLFormatters {
    /** Instance. */
    private static final MLFormatters INSTANCE = new MLFormatters();

    /** Formatters. */
    private Map<Class, ModelFormatter> formatters = new ConcurrentHashMap<>();
    /** Model Formatter Stub. */
    private ModelFormatter stub = new ModelFormatterStub();

    /**
     * Register model formatter.
     *
     * @param modelClass Model class.
     * @param formatter Formatter.
     */
    public <M extends Model> void registerFormatter(Class<M> modelClass, ModelFormatter<M> formatter) {
        formatters.put(modelClass, formatter);
    }

    /**
     * Find formatter for model and map it to string.
     *
     * @param mdl Model.
     */
    public <M extends Model> String format(M mdl) {
        return formatters.getOrDefault(mdl.getClass(), stub).format(mdl);
    }

    /**
     * Returns an instance of MLFormatters.
     */
    public static MLFormatters getInstance() {
        return INSTANCE;
    }

    /**
     * Model formatting stub. Just returns model class name.
     */
    private static class ModelFormatterStub implements ModelFormatter<Model> {
        @Override public String format(Model model) {
            return model.getClass().getName();
        }
    }
}
