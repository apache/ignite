package org.apache.ignite.internal.processors.rest.igfs.model;

/**
 * Range request value object
 */
public class Range {
  private final long start;

  private final long end;

  /**
   * Constructs a new {@link Range}.
   *
   * @param start of range
   * @param end of range
   */
  public Range(final long start, final long end) {
    this.start = start;
    this.end = end;
  }

  /**
   * @return start index of range request
   */
  public long getStart() {
    return start;
  }

  /**
   * @return end index of range request
   */
  public long getEnd() {
    return end;
  }
}