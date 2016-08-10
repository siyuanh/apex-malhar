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
package org.apache.apex.malhar.stream.sample.complete;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.stream.api.impl.accumulation.TopN;
import org.apache.hadoop.conf.Configuration;

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
 * Beam's TopWikipediaSessions Example.
 */
@ApplicationAnnotation(name = "TopWikipediaSessions")
public class TopWikipediaSessions implements StreamingApplication
{
  /**
   * A generator that outputs a stream of combinations of some users and some randomly generated edit time.
   */
  public static class SessionGen extends BaseOperator implements InputOperator
  {
    private String[] names = new String[]{"user1", "user2", "user3", "user4"};
    public transient DefaultOutputPort<KeyValPair<String, Long>> output = new DefaultOutputPort<>();
  
    private static final Duration RAND_RANGE = Duration.standardDays(365);
    private Long minTimestamp;
    
    private String randomName(String[] names)
    {
      int index = new Random().nextInt(names.length);
      return names[index];
    }
  
    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      minTimestamp = System.currentTimeMillis();
    }
  
    @Override
    public void emitTuples()
    {
      long randMillis = (long)(Math.random() * RAND_RANGE.getMillis());
      long randomTimestamp = minTimestamp + randMillis;
      output.emit(new KeyValPair<String, Long>(randomName(names), randomTimestamp));
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  
  /**
   * Convert the upstream (user, time) combination to a timestamped tuple of user.
   */
  static class ExtractUserAndTimestamp implements Function.MapFunction<KeyValPair<String, Long>, Tuple.TimestampedTuple<String>>
  {
    @Override
    public Tuple.TimestampedTuple<String> f(KeyValPair<String, Long> input)
    {
      long timestamp = input.getValue();
      String userName = input.getKey();
   
      // Sets the implicit timestamp field to be used in windowing.
      return new Tuple.TimestampedTuple<>(timestamp, userName);
      
    }
  }
  
  /**
   * Computes the number of edits in each user session.  A session is defined as
   * a string of edits where each is separated from the next by less than an hour.
   */
  static class ComputeSessions
      extends CompositeStreamTransform<Tuple.TimestampedTuple<String>, Tuple.WindowedTuple<KeyValPair<String, Long>>>
  {
    @Override
    public ApexStream<Tuple.WindowedTuple<KeyValPair<String, Long>>> compose(ApexStream<Tuple.TimestampedTuple<String>> inputStream)
    {
      return inputStream
        
        // Chuck the stream into session windows.
        .window(new WindowOption.SessionWindows(Duration.standardHours(1)), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        
        // Count the number of edits for a user within one session.
        .countByKey(new Function.ToKeyValue<Tuple.TimestampedTuple<String>, String, Long>()
        {
          @Override
          public Tuple.TimestampedTuple<KeyValPair<String, Long>> f(Tuple.TimestampedTuple<String> input)
          {
            return new Tuple.TimestampedTuple<KeyValPair<String, Long>>(input.getTimestamp(), new KeyValPair<String, Long>(input.getValue(), 1L));
          }
        }, name("ComputeSessions"));
    }
  }
  
  /**
   * A comparator class used for comparing two TempWrapper objects.
   */
  public static class Comp implements Comparator<TempWrapper>
  {
    @Override
    public int compare(TempWrapper o1, TempWrapper o2)
    {
      return Long.compare(o1.getValue().getValue(), o2.getValue().getValue());
    }
  }
  
  /**
   * A function to extract timestamp from a TempWrapper object.
   */
  // TODO: Need to revisit and change back to using TimestampedTuple.
  public static class TimeStampExtractor implements com.google.common.base.Function<TempWrapper, Long>
  {
    @Override
    public Long apply(@Nullable TempWrapper input)
    {
      return input.getTimeStamp();
    }
  }
  
  /**
   * A temporary wrapper to wrap a KeyValPair and a timestamp together to represent a timestamped tuple, the reason
   * for this is that we cannot resolve a type conflict when calling accumulate(). After the issue resolved, we can
   * remove this class.
   */
  public static class TempWrapper
  {
    private KeyValPair<String, Long> value;
    private Long timeStamp;
    
    public TempWrapper()
    {
      
    }
    
    public TempWrapper(KeyValPair<String, Long> value, Long timeStamp)
    {
      this.value = value;
      this.timeStamp = timeStamp;
    }
  
    public Long getTimeStamp()
    {
      return timeStamp;
    }
  
    public void setTimeStamp(Long timeStamp)
    {
      this.timeStamp = timeStamp;
    }
  
    public KeyValPair<String, Long> getValue()
    {
      return value;
    }
  
    public void setValue(KeyValPair<String, Long> value)
    {
      this.value = value;
    }
  }

  /**
   * Computes the longest session ending in each month, in this case we use 30 days to represent every month.
   */
  private static class TopPerMonth
      extends CompositeStreamTransform<Tuple.WindowedTuple<KeyValPair<String, Long>>, Tuple.WindowedTuple<List<TempWrapper>>>
  {
    
    @Override
    public ApexStream<Tuple.WindowedTuple<List<TempWrapper>>> compose(ApexStream<Tuple.WindowedTuple<KeyValPair<String, Long>>> inputStream)
    {
      TopN<TempWrapper> topN = new TopN<>();
      topN.setN(10);
      topN.setComparator(new Comp());
      
      return inputStream
        
        // Map the input WindowedTuple to a TempWrapper object.
        .map(new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, Long>>, TempWrapper>()
        {
          @Override
          public TempWrapper f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
          {
            return new TempWrapper(input.getValue(), input.getWindows().get(0).getBeginTimestamp());
          }
        }, name("TempWrapper"))
        
        // Apply window and trigger option again, this time chuck the stream into fixed time windows.
        .window(new WindowOption.TimeWindows(Duration.standardDays(30)), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.standardSeconds(5)))
        
        // Compute the top 10 user-sessions with most number of edits.
        .accumulate(topN, name("timestampExtractor")).with("timestampExtractor", new TimeStampExtractor());
    }
  }
  
  /**
   * A map function that combine the user and his/her edit session together to a string and use that string as a key
   * with number of edits in that session as value to create a new key value pair to send to downstream.
   */
  static class SessionsToStringsDoFn implements Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, Long>>, Tuple.WindowedTuple<KeyValPair<String, Long>>>
  {
    @Override
    public Tuple.WindowedTuple<KeyValPair<String, Long>> f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
    {
      return new Tuple.WindowedTuple<KeyValPair<String, Long>>(input.getWindows().get(0), new KeyValPair<String, Long>(
        input.getValue().getKey()  + " : " + input.getWindows().get(0).getBeginTimestamp() + " : " + input.getWindows().get(0).getDurationMillis(),
        input.getValue().getValue()));
    }
  }
  
  /**
   * A flapmap function that turns the result into readable format.
   */
  static class FormatOutputDoFn implements Function.FlatMapFunction<Tuple.WindowedTuple<List<TempWrapper>>, String>
  {
    @Override
    public Iterable<String> f(Tuple.WindowedTuple<List<TempWrapper>> input)
    {
      ArrayList<String> result = new ArrayList<>();
      for (TempWrapper item : input.getValue()) {
        String session = item.getValue().getKey();
        long count = item.getValue().getValue();
        result.add(session + " + " + count + " : " + input.getWindows().get(0).getBeginTimestamp());
      }
      return result;
    }
  }
  
  /**
   * A composite trasnform that compute the top wikipedia sessions.
   */
  public static class ComputeTopSessions extends CompositeStreamTransform<KeyValPair<String, Long>, String>
  {
    @Override
    public ApexStream<String> compose(ApexStream<KeyValPair<String, Long>> inputStream)
    {
      return inputStream
        .map(new ExtractUserAndTimestamp(), name("ExtractUserAndTimestamp"))
        .addCompositeStreams(new ComputeSessions())
        .map(new SessionsToStringsDoFn(), name("SessionsToStringsDoFn"))
        .addCompositeStreams(new TopPerMonth())
        .flatMap(new FormatOutputDoFn(), name("FormatOutputDoFn"));
    }
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SessionGen sg = new SessionGen();
    StreamFactory.fromInput(sg, sg.output, name("sessionGen"))
      .addCompositeStreams(new ComputeTopSessions())
      .print().populateDag(dag);
  }
}
