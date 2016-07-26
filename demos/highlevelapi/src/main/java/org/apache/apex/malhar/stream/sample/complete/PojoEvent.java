package org.apache.apex.malhar.stream.sample.complete;

/**
 * Tuple Class for JdbcOutput of StreamingWordExtract.
 */
public class PojoEvent extends Object
{
  private String string_value;
  
  @Override
  public String toString()
  {
    return "PojoEvent [string_value=" + getString_value() + "]";
  }
  
  public void setString_value(String newString)
  {
    this.string_value = newString;
  }
  
  public String getString_value()
  {
    return this.string_value;
  }
}

