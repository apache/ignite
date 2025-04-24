package org.apache.ignite.internal.processors.rest.igfs.config;


import static java.util.Objects.requireNonNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ignite.internal.processors.rest.igfs.model.Range;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.StringUtils;

/**
 * Converts http range header value to range object
 */
public class RangeConverter implements Converter<String, Range> {
  private static final String REQUESTED_RANGE_REGEXP = "^bytes=((\\d*)\\-(\\d*))((,\\d*-\\d*)*)";

  private static final Pattern REQUESTED_RANGE_PATTERN = Pattern.compile(REQUESTED_RANGE_REGEXP);

  @Override
  public Range convert(String rangeString) {
    requireNonNull(rangeString);

    final Range range;

    // parsing a range specification of format: "bytes=start-end" - multiple ranges not supported
    rangeString = rangeString.trim();
    final Matcher matcher = REQUESTED_RANGE_PATTERN.matcher(rangeString);
    if (matcher.matches()) {
      final String rangeStart = matcher.group(2);
      final String rangeEnd = matcher.group(3);

      range =
          new Range(rangeStart == null ? 0L : Long.parseLong(rangeStart),
              (StringUtils.isEmpty(rangeEnd) ? Long.MAX_VALUE
                  : Long.parseLong(rangeEnd)));

      if (matcher.groupCount() == 5 && !"".equals(matcher.group(4))) {
        throw new IllegalArgumentException(
            "Unsupported range specification. Only single range specifications allowed");
      }
      if (range.getStart() < 0) {
        throw new IllegalArgumentException(
            "Unsupported range specification. A start byte must be supplied");
      }

      if (range.getEnd() != -1 && range.getEnd() < range.getStart()) {
        throw new IllegalArgumentException(
            "Range header is malformed. End byte is smaller than start byte.");
      }
    } else {
      throw new IllegalArgumentException(
          "Range header is malformed. Only bytes supported as range type.");
    }

    return range;
  }
}
