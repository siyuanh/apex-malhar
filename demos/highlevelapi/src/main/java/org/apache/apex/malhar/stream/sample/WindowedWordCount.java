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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.joda.time.Duration;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Throwables;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * Beam WindowedWordCount Example.
 */
@ApplicationAnnotation(name = "WindowedWordCount")
public class WindowedWordCount implements StreamingApplication
{
  static final int WINDOW_SIZE = 1;  // Default window duration in minutes
  
  /**
   * A input operator that reads from and output a file line by line to downstream with a time gap between
   * every two lines.
   */
  public static class TextInput extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
    
    private transient BufferedReader reader;
    
    @Override
    public void setup(Context.OperatorContext context)
    {
      initReader();
    }
    
    private void initReader()
    {
      try {
        InputStream resourceStream = this.getClass().getResourceAsStream("/wordcount/word.txt");
        reader = new BufferedReader(new InputStreamReader(resourceStream));
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }
    
    @Override
    public void teardown()
    {
      IOUtils.closeQuietly(reader);
    }
    
    @Override
    public void emitTuples()
    {
      try {
        String line = reader.readLine();
        if (line == null) {
          reader.close();
          initReader();
        } else {
          this.output.emit(line);
        }
        Thread.sleep(500);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * A Pojo Tuple class used for outputting result to JDBC.
   */
  public static class PojoEvent
  {
    private String word;
    private long count;
    private long timeStamp;
  
    @Override
    public String toString()
    {
      return "PojoEvent [word=" + getWord() + ", count=" + getCount() + ", timeStamp=" + getTimeStamp() + "]";
    }
  
    public String getWord()
    {
      return word;
    }
  
    public void setWord(String word)
    {
      this.word = word;
    }
  
    public long getCount()
    {
      return count;
    }
  
    public void setCount(long count)
    {
      this.count = count;
    }
  
    public long getTimeStamp()
    {
      return timeStamp;
    }
  
    public void setTimeStamp(long timeStamp)
    {
      this.timeStamp = timeStamp;
    }
  }
  
  /**
   * A map function that wrap the input string with a random generated timestamp.
   */
  public static class AddTimestampFn implements Function.MapFunction<String, Tuple.TimestampedTuple<String>>
  {
    private static final Duration RAND_RANGE = Duration.standardMinutes(10);
    private final Long minTimestamp;
    
    AddTimestampFn()
    {
      this.minTimestamp = System.currentTimeMillis();
    }
    
    @Override
    public Tuple.TimestampedTuple<String> f(String input)
    {
      // Generate a timestamp that falls somewhere in the past two hours.
      long randMillis = (long)(Math.random() * RAND_RANGE.getMillis());
      long randomTimestamp = minTimestamp + randMillis;

      return new Tuple.TimestampedTuple<>(randomTimestamp, input);
    }
  }
  
  /** A MapFunction that converts a Word and Count into a PojoEvent. */
  public static class FormatAsTableRowFn implements Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, Long>>, PojoEvent>
  {
    @Override
    public PojoEvent f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
    {
      PojoEvent row = new PojoEvent();
      row.setTimeStamp(input.getTimestamp());
      row.setCount(input.getValue().getValue());
      row.setWord(input.getValue().getKey());
      return row;
    }
  }
  
  /**
   * Populate dag with High-Level API.
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TextInput input = new TextInput();
    
    // Create stream from the TextInput operator.
    ApexStream<Tuple.TimestampedTuple<String>> stream = StreamFactory.fromInput(input, input.output, name("input"))
      
        // Extract all the words from the input line of text.
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split("[\\p{Punct}\\s]+"));
          }
        }, name("ExtractWords"))
      
        // Wrap the word with a randomly generated timestamp.
        .map(new AddTimestampFn(), name("AddTimestampFn"));
    
   
    // apply window and trigger option.
    // TODO: change trigger option to atWaterMark when available.
    WindowedStream<Tuple.TimestampedTuple<String>> windowedWords = stream
        .window(new WindowOption.TimeWindows(Duration.standardMinutes(WINDOW_SIZE)),
        new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.standardSeconds(1)));
    
    
    WindowedStream<PojoEvent> wordCounts =
        // Perform a countByKey transformation to count the appearance of each word in every time window.
        windowedWords.countByKey(new Function.ToKeyValue<Tuple.TimestampedTuple<String>, String, Long>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(Tuple.TimestampedTuple<String> input)
          {
            return new Tuple.TimestampedTuple<KeyValPair<String, Long>>(input.getTimestamp(),
              new KeyValPair<String, Long>(input.getValue(), 1L));
          }
        }, name("count words"))
          
        // Format the output and print out the result.
        .map(new FormatAsTableRowFn(), name("FormatAsTableRowFn")).print();
    
    wordCounts.populateDag(dag);
  }
}
