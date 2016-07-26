package org.apache.apex.malhar.stream.sample.complete;

/**
 * Class used to store tag-count pairs in Auto Complete Demo.
 */
public class CompletionCandidate implements Comparable<CompletionCandidate>
{
  private long count;
  private String value;

  public CompletionCandidate(String value, long count)
  {
    this.value = value;
    this.count = count;
  }

  public long getCount()
  {
    return count;
  }

  public String getValue()
  {
    return value;
  }

  // Empty constructor required for Avro decoding.
  public CompletionCandidate() {}

  @Override
  public int compareTo(CompletionCandidate o)
  {
    if (this.value.equals(o.getValue()) && this.count > o.getCount()) {
      return 0;
    }
    if (this.count < o.count) {
      return -1;
    } else if (this.count == o.count) {
      return this.value.compareTo(o.value);
    } else {
      return 1;
    }
  }

  @Override
  public boolean equals(Object other)
  {
    if (other instanceof CompletionCandidate) {
      CompletionCandidate that = (CompletionCandidate)other;
      return this.count == that.count && this.value.equals(that.value);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode()
  {
    return Long.valueOf(count).hashCode() ^ value.hashCode();
  }

  @Override
  public String toString()
  {
    return "CompletionCandidate[" + value + ", " + count + "]";
  }
}


