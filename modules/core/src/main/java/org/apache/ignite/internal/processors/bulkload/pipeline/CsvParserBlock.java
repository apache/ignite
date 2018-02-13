package org.apache.ignite.internal.processors.bulkload.pipeline;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;

public class CsvParserBlock extends PipelineBlock<char[], String[]> {
    /** Leftover characters from the previous invocation of {@link #accept(char[], boolean)}. */
    private StringBuilder leftover = new StringBuilder();
    private List<String> fields = new ArrayList<>();

    private final byte action[] = new byte[128];

    /**
     * Creates line splitter block.
     */
    public CsvParserBlock() {
        action[','] = 2;
        action['\r'] = 1;
        action['\n'] = 1;
    }

    /** {@inheritDoc} */
    @Override public void accept(char[] chars, boolean isLastPortion) throws IgniteCheckedException {
        leftover.append(chars);

        int lastPos = 0;
        for (int i = 0; i < leftover.length(); i++) {
            char c = leftover.charAt(i);
            byte act = c < 128 ? action[c] : 0;
            switch (act) {
                case 2:
                    fields.add(leftover.substring(lastPos, i));
                    i++;
                    lastPos = i;
                    break;

                case 1:
                    nextBlock.accept(fields.toArray(new String[fields.size()]), false);
                    fields.clear();

                    lastPos = i + 1;
                    if (leftover.charAt(lastPos) == '\n') {
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
            fields.add(leftover.toString());
            leftover.setLength(0);
            nextBlock.accept(fields.toArray(new String[fields.size()]), true);
            fields.clear();
        }
    }

}
