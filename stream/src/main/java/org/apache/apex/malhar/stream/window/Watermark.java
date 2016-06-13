package org.apache.apex.malhar.stream.window;

/**
 * The watermark tuple class
 */
public interface Watermark
{
  /**
   * Gets the timestamp associated with this watermark
   *
   * @return
   */
  long getTimestamp();
}
