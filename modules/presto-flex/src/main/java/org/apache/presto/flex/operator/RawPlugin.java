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
package org.apache.presto.flex.operator;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.presto.flex.FlexColumn;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;

import io.prestosql.spi.PrestoException;

public class RawPlugin implements FilePlugin {
    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        return ImmutableList.of(new FlexColumn("data", VARBINARY));
    }

    @Override
    public List<Object> splitToList(Iterator lines)
    {
    	try {
    		Iterator<InputStream> iterable = (Iterator<InputStream>)lines;
    		InputStream in = iterable.next();
			byte [] all = in.readAllBytes();
			in.close();
			return Arrays.asList(all);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get data"+e.getMessage());
		}
       
    }

    @Override
    public Iterator getIterator(ByteSource byteSource,URI uri)
    {
        try {
            return  Arrays.asList(byteSource.openBufferedStream()).iterator();
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
