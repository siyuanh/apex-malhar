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
package com.datatorrent.demos.wordcount;

import java.util.Arrays;

import org.junit.Test;

import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

/**
 * A embedded application test without creating Streaming Application
 */
public class LocalTestWithoutStreamApplication
{
  @Test
  public void testNonStreamApplicationWordcount() throws Exception
  {
    StreamFactory.
      fromFolder("./src/main/resources/data").
      flatMap(new Function.FlatMapFunction<String, String>()
      {
        @Override
        public Iterable<String> f(String input)
        {
          return Arrays.asList(input.split(" "));
        }
      }).
      countByKey().print().runLocally(5000);
  }
}
