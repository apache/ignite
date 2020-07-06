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

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.ebyhr.presto.flex.FlexColumn;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class ExcelPlugin
        implements FilePlugin
{
    private static final DataFormatter DATA_FORMATTER = new DataFormatter();

    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        try {
            URI uri = URI.create(table);
            ByteSource byteSource = Resources.asByteSource(uri.toURL());
            Workbook workbook = WorkbookFactory.create(byteSource.openStream());
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rows = sheet.iterator();
            List<FlexColumn> columnTypes = new LinkedList<>();
            Row row = rows.next();
            for (Cell cell : row) {
                String cellValue = DATA_FORMATTER.formatCellValue(cell);
                columnTypes.add(new FlexColumn(cellValue, VARCHAR));
            }
            return columnTypes;
        }
        catch (IOException e) {
            throw new RuntimeException(String.format("Failed to operate %s file", table));
        }
    }

    @Override
    public Iterator getIterator(ByteSource byteSource)
    {
        try {
            Workbook workbook = WorkbookFactory.create(byteSource.openStream());
            Sheet sheet = workbook.getSheetAt(0);
            return sheet.iterator();
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to operate s file");
        }
    }

    @Override
    public List<String> splitToList(Iterator lines)
    {
        List<String> values = new ArrayList<>();
        Row row = (Row) lines.next();
        for (Cell cell : row) {
            String cellValue = DATA_FORMATTER.formatCellValue(cell);
            values.add(cellValue);
        }
        return values;
    }

    @Override
    public boolean skipFirstLine()
    {
        return true;
    }
}
