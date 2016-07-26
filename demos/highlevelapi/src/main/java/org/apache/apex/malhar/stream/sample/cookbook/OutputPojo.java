package org.apache.apex.malhar.stream.sample.cookbook;

/**
 * OutputPojo Tuple Class for jdbcOutput.
 */
public class OutputPojo
{
  private int month;
  private double meanTemp;
  
  @Override
  public String toString()
  {
    return "PojoEvent [month=" + getMonth() + ", meanTemp=" + getMeanTemp() + "]";
  }
  
  public void setMonth(int month)
  {
    this.month = month;
  }
  
  public int getMonth()
  {
    return this.month;
  }
  
  public void setMeanTemp(double meanTemp)
  {
    this.meanTemp = meanTemp;
  }
  
  public double getMeanTemp()
  {
    return meanTemp;
  }
}
