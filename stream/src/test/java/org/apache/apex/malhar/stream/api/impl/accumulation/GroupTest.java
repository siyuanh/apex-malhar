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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Test for {@link Group}.
 */
public class GroupTest
{
  
  public static class Collector extends BaseOperator
  {
    private static Tuple.WindowedTuple<List<String>> result;
    private static boolean done = false;
    
    public Tuple.WindowedTuple<List<String>> getResult()
    {
      return result;
    }
    
    public boolean getDone()
    {
      return done;
    }
    
    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      result = new Tuple.WindowedTuple<>();
    }
    
    public transient DefaultInputPort<Tuple.WindowedTuple<List<String>>> input = new DefaultInputPort<Tuple.WindowedTuple<List<String>>>()
    {
      @Override
      public void process(Tuple.WindowedTuple<List<String>> tuple)
      {
        result = tuple;
        if (result.getValue().contains("bye")) {
          done = true;
        }
      }
    };
  }
  
  @Test
  public void GroupTest()
  {
    Group<Integer> group = new Group<>();
    
    List<Integer> accu = group.defaultAccumulatedValue();
    Assert.assertEquals(0, accu.size());
    Assert.assertEquals(1, group.accumulate(accu, 10).size());
    Assert.assertEquals(2, group.accumulate(accu, 11).size());
    Assert.assertEquals(3, group.accumulate(accu, 11).size());
  }
  
  @Test
  public void GroupTestStream()
  {
    final Collector collector = new Collector();
    ApexStream<String> stream = StreamFactory.fromFolder("./src/test/resources/words")
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split("[\\p{Punct}\\s]+"));
          }
        });
    stream
        .window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        .accumulate(new Group<String>())
        .endWith(collector, collector.input)
        .runEmbedded(false, 10000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return collector.getDone();
          }
        });
    
    Assert.assertEquals(37, collector.getResult().getValue().size());
    
  }
}
