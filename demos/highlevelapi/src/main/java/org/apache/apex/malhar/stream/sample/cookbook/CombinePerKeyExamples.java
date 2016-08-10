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

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.stream.api.impl.accumulation.ReduceFn;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * An example that reads the public 'Shakespeare' data, and for each word in
 * the dataset that is over a given length, generates a string containing the
 * list of play names in which that word appears
 *
 * <p>Concepts: the combine transform, which lets you combine the values in a
 * key-grouped Collection
 *
 */
@ApplicationAnnotation(name = "CombinePerKeyExamples")
public class CombinePerKeyExamples implements StreamingApplication
{
  // Use the shakespeare public BigQuery sample
  private static final String SHAKESPEARE_TABLE = "publicdata:samples.shakespeare";
  // We'll track words >= this word length across all plays in the table.
  private static final int MIN_WORD_LENGTH = 0;

  /**
   * Examines each row in the input table. If the word is greater than or equal to MIN_WORD_LENGTH,
   * outputs word, play_name.
   */
  static class ExtractLargeWordsFn implements Function.MapFunction<SampleBean, KeyValPair<String, String>>
  {

    @Override
    public KeyValPair<String, String> f(SampleBean input)
    {
      String playName = input.getCorpus();
      String word = input.getWord();
      if (word.length() >= MIN_WORD_LENGTH) {
        return new KeyValPair<>(word, playName);
      } else {
        return null;
      }
    }
  }


  /**
   * Prepares the output data which is in same bean
   */
  static class FormatShakespeareOutputFn implements Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, String>>, SampleBean>
  {
    @Override
    public SampleBean f(Tuple.WindowedTuple<KeyValPair<String, String>> input)
    {
      return new SampleBean(input.getValue().getKey(), input.getValue().getValue());
    }
  }
  
  /**
   * A reduce function to concat two strings together.
   */
  public static class Concat extends ReduceFn<String>
  {
    @Override
    public String reduce(String input1, String input2)
    {
      return input1 + ", " + input2;
    }
  }
  
  /**
   * Reads the public 'Shakespeare' data, and for each word in the dataset
   * over a given length, generates a string containing the list of play names
   * in which that word appears.
   */
  private static class PlaysForWord extends CompositeStreamTransform<SampleBean, SampleBean>
  {
    
    @Override
    public ApexStream<SampleBean> compose(ApexStream<SampleBean> inputStream)
    {
      return inputStream
          // Extract words from the input SampleBeam stream.
          .map(new ExtractLargeWordsFn(), name("ExtractLargeWordsFn"))
          
          // Apply window and trigger option to the streams.
          .window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        
          // Apply reduceByKey transformation to concat the names of all the plays that a word has appeared in together.
          .reduceByKey(new Concat(), new Function.ToKeyValue<KeyValPair<String,String>, String, String>()
          {
            @Override
            public Tuple<KeyValPair<String, String>> f(KeyValPair<String, String> input)
            {
              return new Tuple.PlainTuple<KeyValPair<String, String>>(input);
            }
          }, name("Concat"))
        
          // Format the output back to a SampleBeam object.
          .map(new FormatShakespeareOutputFn(), name("FormatShakespeareOutputFn"));
    }
  }
  
  
  /**
   * A Java Beam class that contains information about a word appears in a corpus written by Shakespeare.
   */
  public static class SampleBean
  {

    public SampleBean()
    {

    }

    public SampleBean(String word, String corpus)
    {
      this.word = word;
      this.corpus = corpus;
    }
  
    @Override
    public String toString()
    {
      return this.word + " : "  + this.corpus;
    }
  
    private String word;

    private String corpus;

    public void setWord(String word)
    {
      this.word = word;
    }

    public String getWord()
    {
      return word;
    }

    public void setCorpus(String corpus)
    {
      this.corpus = corpus;
    }

    public String getCorpus()
    {
      return corpus;
    }
  }
  
  /**
   * A dummy info generator to generate {@link SampleBean} objects to mimic reading from read 'Shakespeare'
   * data.
   */
  public static class SampleInput extends BaseOperator implements InputOperator
  {

    public final transient DefaultOutputPort<SampleBean> beanOutput = new DefaultOutputPort();
    private String[] words = new String[]{"A", "B", "C", "D", "E", "F", "G"};
    private String[] corpuses = new String[]{"1", "2", "3", "4", "5", "6", "7", "8"};
    
    @Override
    public void emitTuples()
    {
      for (String word : words) {
        for (String corpus : corpuses) {
          beanOutput.emit(new SampleBean(word, corpus));
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    
    }
  }
  
  /**
   * Populate dag using High-Level API.
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SampleInput input = new SampleInput();
    StreamFactory.fromInput(input, input.beanOutput, name("input"))
      .addCompositeStreams(new PlaysForWord())
      .print()
      .populateDag(dag);
    
  }
}
