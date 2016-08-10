package org.apache.apex.malhar.stream.api.impl.accumulation;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * Group accumulation.
 */
public class Group<T> implements Accumulation<T, List<T>, List<T>>
{
  @Override
  public List<T> defaultAccumulatedValue()
  {
    return new ArrayList<>();
  }
  
  @Override
  public List<T> accumulate(List<T> accumulatedValue, T input)
  {
    accumulatedValue.add(input);
    return accumulatedValue;
  }
  
  @Override
  public List<T> merge(List<T> accumulatedValue1, List<T> accumulatedValue2)
  {
    accumulatedValue1.addAll(accumulatedValue2);
    return accumulatedValue1;
  }
  
  @Override
  public List<T> getOutput(List<T> accumulatedValue)
  {
    return accumulatedValue;
  }
  
  @Override
  public List<T> getRetraction(List<T> value)
  {
    // TODO: Need to add implementation for retraction.
    return new ArrayList<>();
  }
}
