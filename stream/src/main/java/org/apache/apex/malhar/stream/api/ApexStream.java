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
package org.apache.apex.malhar.stream.api;


import java.util.Map;

import org.apache.apex.malhar.stream.api.function.Function;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * The stream interface to build a DAG
 * @param <T>
 */
public interface ApexStream<T>
{
  /**
   * Simple map transformation<br>
   * Add an operator to the DAG which convert tuple T to tuple O
   * @param mapFunction map function
   * @param <O> Type of the output
   * @return
   */
  <O, STREAM extends ApexStream<O>> STREAM map(Function.MapFunction<T, O> mapFunction);

  /**
   * Simple map transformation<br>
   * Add an operator to the DAG which convert tuple T to tuple O
   * @param name operator name
   * @param mapFunction map function
   * @param <O> Type of the output
   * @return
   */
  <O, STREAM extends ApexStream<O>> STREAM map(String name, Function.MapFunction<T, O> mapFunction);

  /**
   * Flat map transformation
   * Add an operator to the DAG which convert tuple T to a collection of tuple O
   * @param flatten flat map
   * @param <O> Type of the output
   * @return
   */
  <O, STREAM extends ApexStream<O>> STREAM flatMap(Function.FlatMapFunction<T, O> flatten);

  /**
   * Flat map transformation<br>
   * Add an operator to the DAG which convert tuple T to a collection of tuple O
   * @param name operator name
   * @param flatten
   * @param <O> Type of the output
   * @return
   */
  <O, STREAM extends ApexStream<O>> STREAM flatMap(String name, Function.FlatMapFunction<T, O> flatten);

  /**
   * Filter transformation<br>
   * Add an operator to the DAG which filter out tuple T that cannot satisfy the FilterFunction
   * @param filter filter function
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM filter(Function.FilterFunction<T> filter);

  /**
   * Filter transformation<br>
   * Add an operator to the DAG which filter out tuple T that cannot satisfy the FilterFunction
   * @param name operator name
   * @param filter filter function
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM filter(String name, Function.FilterFunction<T> filter);

  /**
   * Reduce transformation<br>
   * Add an operator to the DAG which merge tuple t1, t2 to new tuple
   * @param reduce reduce function
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM reduce(Function.ReduceFunction reduce);

  /**
   * Reduce transformation<br>
   * Add an operator to the DAG which merge tuple t1, t2 to new tuple
   * @param name operator name
   * @param reduce reduce function
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM reduce(String name, Function.ReduceFunction reduce);

  /**
   * Fold transformation<br>
   * Add an operator to the DAG which merge tuple T to accumulated result tuple O
   * @param initialValue initial result value
   * @param fold fold function
   * @param <O> Result type
   * @return
   */
  <O, STREAM extends ApexStream<O>> STREAM fold(O initialValue, Function.FoldFunction fold);

  /**
   * Fold transformation<br>
   * Add an operator to the DAG which merge tuple T to accumulated result tuple O
   * @param name name of the operator
   * @param initialValue initial result value
   * @param fold fold function
   * @param <O> Result type
   * @return
   */
  <O, STREAM extends ApexStream<O>> STREAM fold(String name, O initialValue, Function.FoldFunction fold);

  /**
   * Count of all tuples
   * @return
   */
  <STREAM extends ApexStream<Integer>> STREAM count();

  /**
   * Count tuples by the key<br>
   * If the input is KeyedTuple it will get the key from getKey method from the tuple<br>
   * If not, use the tuple itself as a key
   * @param <O>
   * @return
   */
  <STREAM extends ApexStream<Map<Object, Integer>>> STREAM countByKey();

  /**
   *
   * Count tuples by the indexed key
   * @param key
   * @param <O>
   * @return
   */
  <STREAM extends ApexStream<Map<Object, Integer>>> STREAM countByKey(int key);

  /**
   * Extend the dag by adding one operator<br>
   * @param op Operator added to the stream
   * @param inputPort InputPort of the operator that is connected to last exposed OutputPort in the stream
   * @param outputPort OutputPort of the operator will be connected to next operator
   * @param <O> type of the output
   * @return
   */
  <O, STREAM extends ApexStream<O>> STREAM addOperator(Operator op, Operator.InputPort<T> inputPort,  Operator.OutputPort<O> outputPort);

  /**
   * Extend the dag by adding one {@see Operator}
   * @param opName
   * @param op
   * @param inputPort
   * @param outputPort
   * @param <O>
   * @return
   */
  <O, STREAM extends ApexStream<O>> STREAM addOperator(String opName, Operator op, Operator.InputPort<T> inputPort,  Operator.OutputPort<O> outputPort);

  /**
   * Union multiple stream into one
   * @param others
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM union(ApexStream<T>... others);

  /**
   * Add a stdout console output operator
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM print();

  /**
   * Add a stderr console output operator
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM printErr();

  /**
   * Set the attribute value<br>
   * If it is {@link DAGContext DAG attribute}, it will be applied to the whole DAG <br>
   * If it is {@link OperatorContext Operator attribute}, it will be applied to last connected operator<br>
   * If it is {@link PortContext InputPort attribute}, it will be applied to the input port of the last connected stream<br>
   * If it is {@link PortContext OutputPort attribute}, it will be applied to the output port of the last connected stream<br>
   * If it is both {@link PortContext InputPort&OutputPort attribute}, it will be applied to last connected stream
   * @param attribute {@see Attribute}
   * @param value
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM with(Attribute attribute, Object value);

  /**
   * Set attributes at the DAG level
   * @param attribute
   * @param value
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM setGlobalAttribute(Attribute attribute, Object value);

  /**
   * Set the locality
   * @param locality
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM with(DAG.Locality locality);

  /**
   * Set the property value of the last connected operator
   * @param propName
   * @param value
   * @return
   */
  <STREAM extends ApexStream<T>> STREAM with(String propName, Object value);


  /**
   * Create dag from stream
   * @return
   */
  DAG createDag();

  /**
   * Populate existing dag
   * @param {@see DAG}
   * @return
   */
  void populateDag(DAG dag);


  /**
   * Run the stream application in local mode
   * @param async
   */
  void runEmbedded(boolean async);

  /**
   * Submit the application to console
   */
  void run();

  /**
   * Run the stream application in local mode in <b>duration</b> milliseconds
   * @param duration
   */
  void runLocally(long duration);
}
