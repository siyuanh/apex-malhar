package org.apache.apex.malhar.stream.api.impl.accumulation;

import java.util.Comparator;
import org.apache.apex.malhar.lib.window.Accumulation;

/**
 * Max accumulation.
 */
public class Max<V> implements Accumulation<V, V, V>
{
  
  Comparator<V> comparator;
  
  public void setComparator(Comparator<V> comparator)
  {
    this.comparator = comparator;
  }
  
  @Override
  public V defaultAccumulatedValue()
  {
    return null;
  }
  
  @Override
  public V accumulate(V accumulatedValue, V input)
  {
    if (accumulatedValue == null) {
      return input;
    } else if (comparator != null) {
      return (comparator.compare(input, accumulatedValue) > 0) ? input : accumulatedValue;
    } else if (input instanceof Comparable) {
      return (((Comparable)input).compareTo(accumulatedValue) > 0) ? input : accumulatedValue;
    } else {
      throw new RuntimeException("Tuple cannot be compared");
    }
  }
  
  @Override
  public V merge(V accumulatedValue1, V accumulatedValue2)
  {
    return accumulate(accumulatedValue1, accumulatedValue2);
  }
  
  @Override
  public V getOutput(V accumulatedValue)
  {
    return accumulatedValue;
  }
  
  @Override
  public V getRetraction(V value)
  {
    return null;
  }
}
