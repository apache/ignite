/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.math.exceptions.knn;

import java.nio.file.Path;
import org.apache.ignite.IgniteException;

/**
 * Shows non-parsed data in specific row by given file path.
 */
public class FileParsingException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception.
     * @param parsedData Data to parse.
     * @param rowIdx Index of row in file.
     * @param file File path
     */
    public FileParsingException(String parsedData, int rowIdx, Path file) {
        super("Data " + parsedData + " in row # " + rowIdx + " in file " + file + " can not be parsed to appropriate format");
    }
}
