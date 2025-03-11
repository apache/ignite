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

package org.apache.ignite.internal.processors.performancestatistics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RowReader {
    private final Function<ByteBuffer, String> stringReader;
    List<Function<ByteBuffer, Object>> columns = new ArrayList<>();
    public RowReader(Function<ByteBuffer, String> supplier) {
        stringReader = supplier;
    }

    public void addType(String type) {
        switch (type) {
            case "int":
                columns.add(ByteBuffer::getInt);
                break;
            case "char":
                columns.add(ByteBuffer::getChar);
                break;
            case "boolean":
                columns.add(buffer -> buffer.get() == 1);
                break;
            case "float":
                columns.add(ByteBuffer::getFloat);
                break;
            case "double":
                columns.add(ByteBuffer::getDouble);
                break;
            case "byte":
                columns.add(ByteBuffer::get);
                break;
            case "short":
                columns.add(ByteBuffer::getShort);
                break;
            case "long":
                columns.add(ByteBuffer::getLong);
                break;
            case "String":
                columns.add(stringReader::apply);
            default:
                throw new UnsupportedOperationException();
        }
    }

    List<Object> readRow(ByteBuffer buf) {
        return columns.stream().map(function -> function.apply(buf)).collect(Collectors.toList());
    }
}
