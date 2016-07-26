package org.apache.apex.malhar.stream.sample.cookbook;

/**
 * Tuple class for JDBC input.
 */
public class InputPojo extends Object
{
  private int month;
  private int day;
  private int year;
  private double meanTemp;
  
  @Override
  public String toString()
  {
    return "PojoEvent [month=" + getMonth() + ", day=" + getDay() + ", year=" + getYear() + ", meanTemp=" + getMeanTemp() + "]";
  }
  
  public void setMonth(int month)
  {
    this.month = month;
  }
  
  public int getMonth()
  {
    return this.month;
  }
  
  public void setDay(int day)
  {
    this.day = day;
  }
  
  public int getDay()
  {
    return day;
  }
  
  public void setYear(int year)
  {
    this.year = year;
  }
  
  public int getYear()
  {
    return year;
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
