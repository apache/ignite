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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class Formatters {
    private static final Formatters INSTANCE = new Formatters();

    private Map<Class, ModelFormatter> formatters = new ConcurrentHashMap<>();
    private ModelFormatter stub = new ModelFormatterStub();
    private AtomicReference<IgniteFunction<Vector, String>> vectorFormatter = new AtomicReference<>();

    public void registerFormatter(IgniteFunction<Vector, String> vectorFormatter) {
        this.vectorFormatter.set(vectorFormatter);
    }

    public <M extends Model> void registerFormatter(Class<M> modelClass, ModelFormatter<M> formatter) {
        formatters.put(modelClass, formatter);
    }

    public <M extends Model> String format(M mdl) {
        return formatters.getOrDefault(mdl.getClass(), stub).format(mdl);
    }

    public String format(Vector vector) {
        IgniteFunction<Vector, java.lang.String> formatter = vectorFormatter.get();
        if(formatter == null)
            return vector.toString();
        else
            return formatter.apply(vector);
    }


    public static Formatters getInstance() {
        return INSTANCE;
    }

    private static class ModelFormatterStub implements ModelFormatter<Model> {
        @Override public String format(Model model) {
            return model.getClass().getName();
        }
    }
}
