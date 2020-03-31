/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.presto.flex.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.facebook.presto.spi.PrestoException;
import org.ebyhr.presto.flex.FlexColumn;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TextPlugin implements FilePlugin {
    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        return ImmutableList.of(new FlexColumn("value", VARCHAR));
    }

    @Override
    public List<String> splitToList(Iterator lines)
    {
        String line = (String) lines.next();
        return Arrays.asList(line);
    }

    @Override
    public Iterator<String> getIterator(ByteSource byteSource)
    {
        try {
            return byteSource.asCharSource(UTF_8).readLines().iterator();
        } catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get iterator");
        }
    }

    @Override
    public boolean skipFirstLine()
    {
        return false;
    }
}
