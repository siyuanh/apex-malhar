package org.apache.apex.malhar.stream.api.impl.accumulation;

import java.util.Comparator;
import java.util.concurrent.Callable;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Test for Max accumulation
 */
public class MaxTest
{
  
  public static class NumGen extends BaseOperator implements InputOperator
  {
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<>();
    
    public static int count = 0;
    private int i = 0;
    
    @Override
    public void emitTuples()
    {
      while (i <= 7) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        count++;
        output.emit(i++);
      }
      i = 0;
    }
  }
  
  public static class Collector extends BaseOperator
  {
    private static int max;
    
    public transient DefaultInputPort<Tuple.WindowedTuple<Integer>> input = new DefaultInputPort<Tuple.WindowedTuple<Integer>>()
    {
      @Override
      public void process(Tuple.WindowedTuple<Integer> tuple)
      {
        max = tuple.getValue();
      }
    };
    
    public int getMax()
    {
      return max;
    }
  }
  
  
  @Test
  public void MaxTest()
  {
    Max<Integer> max = new Max<>();
    
    Assert.assertEquals((Integer)5, max.accumulate(5, 3));
    Assert.assertEquals((Integer)6, max.accumulate(4, 6));
    Assert.assertEquals((Integer)5, max.merge(5, 2));
  
    Comparator<Integer> com = new Comparator<Integer>()
    {
      @Override
      public int compare(Integer o1, Integer o2)
      {
        return -(o1.compareTo(o2));
      }
    };
    
    max.setComparator(com);
    Assert.assertEquals((Integer)3, max.accumulate(5, 3));
    Assert.assertEquals((Integer)4, max.accumulate(4, 6));
    Assert.assertEquals((Integer)2, max.merge(5, 2));
  }
  
  
  @Test
  public void MaxTestWithStream()
  {
    NumGen numGen = new NumGen();
    Collector collector = new Collector();
    ApexStream<Integer> stream = StreamFactory.fromInput(numGen, numGen.output);
    stream.window(new WindowOption.GlobalWindow(), new TriggerOption().withEarlyFiringsAtEvery(Duration.standardSeconds(2)))
        .accumulate(new Max<Integer>()).addOperator(collector, collector.input, null).runEmbedded(false, 600000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return NumGen.count > 25;
          }
        });
    
    Assert.assertEquals(collector.getMax(), 7);
    
  }
}
