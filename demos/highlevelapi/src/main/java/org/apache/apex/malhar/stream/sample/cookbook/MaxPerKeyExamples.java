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

import java.util.List;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.stream.api.impl.accumulation.Max;
import org.apache.hadoop.conf.Configuration;

import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;

import com.google.common.collect.Lists;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.db.jdbc.JdbcFieldInfo;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInputOperator;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.KeyValPair;

/**
 * MaxPerKeyExamples Application from Beam
 */
@ApplicationAnnotation(name = "MaxPerKeyExamples")
public class MaxPerKeyExamples implements StreamingApplication
{
 
  public static class ExtractTempFn implements Function.MapFunction<InputPojo, KeyValPair<Integer, Double>>
  {
    @Override
    public KeyValPair<Integer, Double> f(InputPojo row)
    {
      Integer month = row.getMonth();
      Double meanTemp = row.getMeanTemp();
      return new KeyValPair<Integer, Double>(month, meanTemp);
    }
  }
  
  
  public static class FormatMaxesFn implements Function.MapFunction<Tuple.WindowedTuple<KeyValPair<Integer, Double>>, OutputPojo>
  {
    @Override
    public OutputPojo f(Tuple.WindowedTuple<KeyValPair<Integer, Double>> input)
    {
      OutputPojo row = new OutputPojo();
      row.setMonth(input.getValue().getKey());
      row.setMeanTemp(input.getValue().getValue());
      return row;
    }
  }
  
  
  public static class mp implements Function.MapFunction<KeyValPair<Integer, Double>, Tuple<KeyValPair<Integer, Double>>>
  {
    @Override
    public Tuple<KeyValPair<Integer, Double>> f(KeyValPair<Integer, Double> input)
    {
      return new Tuple.WindowedTuple<KeyValPair<Integer, Double>>(Window.GLOBAL_WINDOW, input);
    }
  }
  
  public static class MaxMeanTemp extends CompositeStreamTransform<InputPojo, OutputPojo>
  {
    @Override
    public ApexStream<OutputPojo> compose(ApexStream<InputPojo> rows)
    {
      // row... => <month, meanTemp> ...
      ApexStream<KeyValPair<Integer, Double>> temps = rows.map(new ExtractTempFn());
      
      // month, meanTemp... => <month, max mean temp>...
      WindowedStream<Tuple.WindowedTuple<KeyValPair<Integer, Double>>> tempMaxes =
          ((WindowedStream<KeyValPair<Integer, Double>>)temps).accumulateByKey(new Max<Double>(), new mp());
      
      // <month, max>... => row...
      WindowedStream<OutputPojo> results = tempMaxes.map(new FormatMaxesFn());
      
      return results;
    }
  }
  
  private List<FieldInfo> addInputFieldInfos()
  {
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("MONTH", "month", FieldInfo.SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("DAY", "day", FieldInfo.SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("YEAR", "year", FieldInfo.SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("MEANTEMP", "meanTemp", FieldInfo.SupportType.DOUBLE));
    return fieldInfos;
  }
  
  private List<JdbcFieldInfo> addOutputFieldInfos()
  {
    List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new JdbcFieldInfo("MONTH", "month", JdbcFieldInfo.SupportType.INTEGER, INTEGER));
    fieldInfos.add(new JdbcFieldInfo("MEANTEMP", "meanTemp", JdbcFieldInfo.SupportType.DOUBLE, DOUBLE));
    return fieldInfos;
  }
  
 
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOInputOperator jdbcInput = new JdbcPOJOInputOperator();
    jdbcInput.setFieldInfos(addInputFieldInfos());
  
    JdbcStore store = new JdbcStore();
    jdbcInput.setStore(store);
  
    JdbcPOJOInsertOutputOperator jdbcOutput = new JdbcPOJOInsertOutputOperator();
    jdbcOutput.setFieldInfos(addOutputFieldInfos());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbcOutput.setStore(outputStore);
    
    ApexStream<Object> stream = StreamFactory.fromInput("jdbcInput", jdbcInput, jdbcInput.outputPort)
        .window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        .map(new Function.MapFunction<Object, InputPojo>()
        {
          @Override
          public InputPojo f(Object input)
          {
            return (InputPojo)input;
          }
        })
        .addCompositeStreams(new MaxMeanTemp())
        .map(new Function.MapFunction<OutputPojo, Object>()
        {
          @Override
          public Object f(OutputPojo input)
          {
            return (Object)input;
          }
        })
        .addOperator("jdbcOutput", jdbcOutput, jdbcOutput.input, null);
    
    stream.populateDag(dag);
  
  }
}
