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
package org.apache.apex.malhar.stream.sample.cookbook;

import java.util.Arrays;
import java.util.List;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.stream.api.impl.accumulation.RemoveDuplicates;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * Beam DeDupExample.
 */
@ApplicationAnnotation(name = "DeDupExample")
public class DeDupExample implements StreamingApplication
{
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Create a stream that reads from files in a local folder and output lines one by one to downstream.
    ApexStream<String> stream = StreamFactory.fromFolder("./src/test/resources/wordcount", name("textInput"))
      
        // Extract all the words from the input line of text.
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split("[\\p{Punct}\\s]+"));
          }
        }, name("ExtractWords"))
      
        // Change the words to lower case, also shutdown the app when the word "bye" is detected.
        .map(new Function.MapFunction<String, String>()
        {
          @Override
          public String f(String input)
          {
            if (input.equals("bye")) {
              throw new Operator.ShutdownException();
            }
            return input.toLowerCase();
          }
        }, name("ToLowerCase"));
    
    // Apply window and trigger option.
    WindowedStream<Tuple.WindowedTuple<List<String>>> result = stream.window(new WindowOption.GlobalWindow(),
        new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.standardSeconds(1)))
        
        // Remove the duplicate words and print out the result.
        .accumulate(new RemoveDuplicates<String>(), name("RemoveDuplicates")).print();
    
    result.populateDag(dag);
  }
}
