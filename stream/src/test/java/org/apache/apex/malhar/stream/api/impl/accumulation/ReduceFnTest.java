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

import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Test;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

/**
 * Test for {@link ReduceFn}.
 */
public class ReduceFnTest
{
  
  public static class Plus extends ReduceFn<Integer>
  {
    @Override
    public Integer reduce(Integer input1, Integer input2)
    {
      return input1 + input2;
    }
  }
  
  @Test
  public void ReduceFnTest()
  {
    ReduceFn<String> concat = new ReduceFn<String>()
    {
      @Override
      public String reduce(String input1, String input2)
      {
        return input1 + ", " + input2;
      }
    };
    
    String[] ss = new String[]{"b", "c", "d", "e"};
    String base = "a";
    
    for (String s : ss) {
      base = concat.accumulate(base, s);
    }
    Assert.assertEquals("a, b, c, d, e", base);
  }
  
  @Test
  public void ReduceFnTestStream()
  {
    
    final FoldFnTest.NumGen numGen = new FoldFnTest.NumGen();
    FoldFnTest.Collector collector = new FoldFnTest.Collector();
  
    StreamFactory.fromInput(numGen, numGen.output).window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        .reduce(new Plus()).endWith(collector, collector.input).runEmbedded(false, 10000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return numGen.count > 10;
          }
        });
    
    Assert.assertEquals(28, collector.getResult());
  }
}
