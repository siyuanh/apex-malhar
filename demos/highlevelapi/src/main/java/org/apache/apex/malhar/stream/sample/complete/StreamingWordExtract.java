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
import java.util.Arrays;
import java.util.List;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.db.jdbc.JdbcFieldInfo;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;

import static java.sql.Types.VARCHAR;

/**
 * Beam StreamingWordExtract Example.
 */
@ApplicationAnnotation(name = "StreamingWordExtract")
public class StreamingWordExtract implements StreamingApplication
{
  private static int wordCount = 0;
  private static int entriesMapped = 0;
  
  public int getWordCount()
  {
    return wordCount;
  }
  
  public int getEntriesMapped()
  {
    return entriesMapped;
  }
  
  /**
   * A MapFunction that tokenizes lines of text into individual words.
   */
  public static class ExtractWords implements Function.FlatMapFunction<String, String>
  {
    @Override
    public Iterable<String> f(String input)
    {
      List<String> result = new ArrayList<>(Arrays.asList(input.split("[^a-zA-Z0-9']+")));
      wordCount += result.size();
      return result;
    }
  }
  
  
  /**
   * A MapFunction that uppercases a word.
   */
  public static class Uppercase implements Function.MapFunction<String, String>
  {
    @Override
    public String f(String input)
    {
      return input.toUpperCase();
    }
  }
  
  
  public static class EmptyStringFilter implements Function.FilterFunction<String>
  {
  
    @Override
    public Boolean f(String input)
    {
      return !input.isEmpty();
    }
  }
  
  
  public static class PojoMapper implements Function.MapFunction<String, Object>
  {
  
    @Override
    public Object f(String input)
    {
      PojoEvent pojo = new PojoEvent();
      pojo.setString_value(input);
      entriesMapped++;
      return pojo;
    }
  }
  
  private static List<JdbcFieldInfo> addFieldInfos()
  {
    List<JdbcFieldInfo> fieldInfos = new ArrayList<>();
    fieldInfos.add(new JdbcFieldInfo("STRING_VALUE", "string_value", JdbcFieldInfo.SupportType.STRING, VARCHAR));
    return fieldInfos;
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOInsertOutputOperator jdbcOutput = new JdbcPOJOInsertOutputOperator();
    jdbcOutput.setFieldInfos(addFieldInfos());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbcOutput.setStore(outputStore);
    jdbcOutput.setTablename("WordExtractTest");
    
    ApexStream<String> stream = StreamFactory.fromFolder("./src/test/resources/data");

    stream.flatMap(new ExtractWords()).filter(new EmptyStringFilter()).map(new Uppercase()).map(new PojoMapper())
        .addOperator("jdbcOutput",jdbcOutput, jdbcOutput.input, null);
    
    stream.populateDag(dag);
  }
}
