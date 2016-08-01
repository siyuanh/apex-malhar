/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
 * Test for {@link Min}.
 */
public class MinTest
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
    private static int min;
    
    public transient DefaultInputPort<Tuple.WindowedTuple<Integer>> input = new DefaultInputPort<Tuple.WindowedTuple<Integer>>()
    {
      @Override
      public void process(Tuple.WindowedTuple<Integer> tuple)
      {
        min = tuple.getValue();
      }
    };
    
    public int getMin()
    {
      return min;
    }
  }
  
  
  @Test
  public void MinTest()
  {
    Min<Integer> min = new Min<>();
    
    Assert.assertEquals((Integer)3, min.accumulate(5, 3));
    Assert.assertEquals((Integer)4, min.accumulate(4, 6));
    Assert.assertEquals((Integer)2, min.merge(5, 2));
    
    Comparator<Integer> com = new Comparator<Integer>()
    {
      @Override
      public int compare(Integer o1, Integer o2)
      {
        return -(o1.compareTo(o2));
      }
    };
    
    min.setComparator(com);
    Assert.assertEquals((Integer)5, min.accumulate(5, 3));
    Assert.assertEquals((Integer)6, min.accumulate(4, 6));
    Assert.assertEquals((Integer)5, min.merge(5, 2));
  }
  
  
  @Test
  public void MaxTestWithStream()
  {
    NumGen numGen = new NumGen();
    Collector collector = new Collector();
    ApexStream<Integer> stream = StreamFactory.fromInput(numGen, numGen.output);
    stream.window(new WindowOption.GlobalWindow(), new TriggerOption().withEarlyFiringsAtEvery(Duration.standardSeconds(2)))
        .accumulate(new Min<Integer>()).addOperator(collector, collector.input, null).runEmbedded(false, 600000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return MinTest.NumGen.count > 25;
          }
        });
    
    Assert.assertEquals(collector.getMin(), 0);
    
  }
}
