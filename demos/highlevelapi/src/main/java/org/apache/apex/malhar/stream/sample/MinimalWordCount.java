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
package org.apache.apex.malhar.stream.sample;

import java.util.Arrays;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Beam MinimalWordCount Example
 */
public class MinimalWordCount implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
 
    ApexStream<String> stream = StreamFactory.fromFolder("./src/test/resources/wordcount")
        .flatMap("ExtractWords", new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split("[^a-zA-Z']+"));
          
          }
        })
        .window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        .countByKey("countByKey", new Function.MapFunction<String, Tuple<KeyValPair<String, Long>>>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(String input)
          {
            return new Tuple.PlainTuple<KeyValPair<String, Long>>(new KeyValPair<String, Long>(input, 1L));
          }
        })
      
        .map("FormatResults", new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, Long>>, String>()
        {
          @Override
          public String f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
          {
            return input.getValue().getKey() + ": " + input.getValue().getValue();
          }
        })
        .print();
    
    stream.populateDag(dag);
  }
}
