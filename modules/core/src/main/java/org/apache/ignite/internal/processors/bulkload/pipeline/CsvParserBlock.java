package org.apache.ignite.internal.processors.bulkload.pipeline;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.NotNull;

/**
 * Parses CSV file by processing both lines and fields. Unifinished fields and lines
 * are kept between invocations of {@link #accept(char[], boolean)}.
 */
public class CsvParserBlock extends PipelineBlock<char[], List<Object>> {
    /** Leftover characters from the previous invocation of {@link #accept(char[], boolean)}. */
    private StringBuilder leftover;

    /** Current parsed fields from the beginning of the line. */
    private List<Object> fields;

    /**
     * Creates line splitter block.
     */
    public CsvParserBlock() {
        leftover = new StringBuilder();
        fields = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override public void accept(char[] chars, boolean isLastPortion) throws IgniteCheckedException {
        leftover.append(chars);

        int lastPos = 0;

        for (int i = 0; i < leftover.length(); i++) {
            switch (leftover.charAt(i)) {
                case ',':
                    fields.add(substrWithoutQuotes(leftover, lastPos, i));

                    lastPos = i + 1;

                    break;

                case '\r':
                case '\n':
                    fields.add(substrWithoutQuotes(leftover, lastPos, i));

                    nextBlock.accept(new ArrayList<>(fields), false);

                    fields.clear();

                    lastPos = i + 1;

                    if (lastPos < leftover.length() && leftover.charAt(lastPos) == '\n') {
                        lastPos++;
                        i++;
                    }

                    break;
            }
        }

        if (lastPos >= leftover.length())
            leftover.setLength(0);
        else if (lastPos != 0)
            leftover.delete(0, lastPos);

        if (isLastPortion && leftover.length() > 0) {
            fields.add(substrWithoutQuotes(leftover, 0, leftover.length()));

            leftover.setLength(0);

            nextBlock.accept(new ArrayList<>(fields), true);

            fields.clear();
        }
    }

    /**
     * Takes substring from the {@code str}, omitting quotes if they are present.
     *
     * @param str The string to take substring from.
     * @param from The beginning index.
     * @param to The end index (last character position + 1).
     * @return The substring without quotes.
     */
    @NotNull private String substrWithoutQuotes(@NotNull StringBuilder str, int from, int to) {
        if ((to - from) >= 2 && str.charAt(from) == '"' && str.charAt(to - 1) == '"')
            return str.substring(from + 1, to - 1);

        return str.substring(from, to);
    }
}
