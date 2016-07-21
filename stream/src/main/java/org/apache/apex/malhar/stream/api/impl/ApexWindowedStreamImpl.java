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
package org.apache.apex.malhar.stream.api.impl;

import java.util.List;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;

import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;

import org.apache.apex.malhar.stream.api.impl.accumulation.Count;
import org.apache.apex.malhar.stream.api.impl.accumulation.FoldFn;
import org.apache.apex.malhar.stream.api.impl.accumulation.ReduceFn;
import org.apache.apex.malhar.stream.api.impl.accumulation.TopN;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.lib.util.KeyValPair;

/**
 * Default windowed stream implementation for WindowedStream interface.
 * It adds more windowed transform for Stream interface
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class ApexWindowedStreamImpl<T> extends ApexStreamImpl<T> implements WindowedStream<T>
{

  protected WindowOption windowOption;

  protected TriggerOption triggerOption;

  protected Duration allowedLateness;

  private class ConvertFn<T> implements Function.MapFunction<T, Tuple<T>>
  {

    @Override
    public Tuple<T> f(T input)
    {
      if (input instanceof Tuple.TimestampedTuple) {
        return (Tuple.TimestampedTuple)input;
      } else {
        return new Tuple.TimestampedTuple<>(System.currentTimeMillis(), input);
      }
    }
  }


  public ApexWindowedStreamImpl()
  {
  }

  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<Long>>> STREAM count()
  {
    return count(null);
  }

  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<Long>>> STREAM count(String name)
  {
    Function.MapFunction<T, Tuple<Long>> kVMap = new Function.MapFunction<T, Tuple<Long>>()
    {
      @Override
      public Tuple<Long> f(T input)
      {
        if (input instanceof Tuple.TimestampedTuple) {
          return new Tuple.TimestampedTuple<>(((Tuple.TimestampedTuple)input).getTimestamp(), 1L);
        } else {
          return new Tuple.TimestampedTuple<>(System.currentTimeMillis(), 1L);
        }
      }
    };
    if (name != null) {
      WindowedStream<Tuple<Long>> innerstream = map(name + "_premap", kVMap);
      WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createWindowedOperator(new Count());
      return innerstream.addOperator(name, windowedOperator, windowedOperator.input, windowedOperator.output);
    } else {
      WindowedStream<Tuple<Long>> innerstream = map(kVMap);
      WindowedOperatorImpl<Long, MutableLong, Long> windowedOperator = createWindowedOperator(new Count());
      return innerstream.addOperator(windowedOperator, windowedOperator.input, windowedOperator.output);
    }
  }

  @Override
  public <K, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, Long>>>> STREAM countByKey(Function.MapFunction<T, Tuple<KeyValPair<K, Long>>> convertToKeyValue)
  {
    return countByKey(null, convertToKeyValue);
  }

  @Override
  public <K, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, Long>>>> STREAM countByKey(String name, Function.MapFunction<T, Tuple<KeyValPair<K, Long>>> convertToKeyValue)
  {

    Count c = new Count();
    WindowedStream<Tuple<KeyValPair<K, Long>>> kvstream = map(name != null ? name + "_premap" : convertToKeyValue.toString(), convertToKeyValue);
    KeyedWindowedOperatorImpl<K, Long, MutableLong, Long> keyedWindowedOperator = createKeyedWindowedOperator(new Count());
    return kvstream.addOperator(name == null ? c.toString() : name, keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output);
  }

  @Override
  public <K, V, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, List<V>>>>> STREAM topByKey(int N, Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    return topByKey(N, null, convertToKeyVal);
  }

  @Override
  public <K, V, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, List<V>>>>> STREAM topByKey(int N, String name, Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    TopN<V> top = new TopN<>();
    top.setN(N);
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(name != null ? name + "_premap" : convertToKeyVal.toString(), convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, List<V>, List<V>> keyedWindowedOperator = createKeyedWindowedOperator(top);
    return kvstream.addOperator(name == null ? top.toString() : name, keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output);
  }

  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<List<T>>>> STREAM top(int N, String name)
  {

    TopN<T> top = new TopN<>();
    top.setN(N);
    WindowedStream<Tuple<T>> innerstream = map((name != null) ? name + "_premap" : name, new ConvertFn<T>());
    WindowedOperatorImpl<T, List<T>, List<T>> windowedOperator = createWindowedOperator(top);
    return innerstream.addOperator((name == null) ? top.toString() : name, windowedOperator, windowedOperator.input, windowedOperator.output);
  }

  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<List<T>>>> STREAM top(int N)
  {
    return top(N, null);
  }

  @Override
  public <K, V, O, ACCU, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, O>>>> STREAM accumulateByKey(Accumulation<V, ACCU, O> accumulation,
      Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    return accumulateByKey(null, accumulation, convertToKeyVal);
  }

  @Override
  public <K, V, O, ACCU, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, O>>>> STREAM accumulateByKey(String name, Accumulation<V, ACCU, O> accumulation,
      Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(name != null ? name + "_premap" : convertToKeyVal.toString(), convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, ACCU, O> keyedWindowedOperator = createKeyedWindowedOperator(accumulation);
    return kvstream.addOperator(name == null ? accumulation.toString() : name, keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output);
  }

  @Override
  public <O, ACCU, STREAM extends WindowedStream<Tuple.WindowedTuple<O>>> STREAM accumulate(Accumulation<T, ACCU, O> accumulation)
  {
    return accumulate(null, accumulation);
  }

  @Override
  public <O, ACCU, STREAM extends WindowedStream<Tuple.WindowedTuple<O>>> STREAM accumulate(String name,
      Accumulation<T, ACCU, O> accumulation)
  {
    WindowedStream<Tuple<T>> innerstream = map(name != null ? name + "_premap" : name, new ConvertFn<T>());
    WindowedOperatorImpl<T, ACCU, O> windowedOperator = createWindowedOperator(accumulation);
    return innerstream.addOperator(name == null ? accumulation.toString() : name, windowedOperator, windowedOperator.input, windowedOperator.output);
  }

  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<T>>> STREAM reduce(ReduceFn<T> reduce)
  {
    return reduce(null, reduce);
  }

  @Override
  public <STREAM extends WindowedStream<Tuple.WindowedTuple<T>>> STREAM reduce(String name, ReduceFn<T> reduce)
  {
    WindowedStream<Tuple<T>> innerstream = map(name != null ? name + "_premap" : name, new ConvertFn<T>());
    WindowedOperatorImpl<T, T, T> windowedOperator = createWindowedOperator(reduce);
    return innerstream.addOperator(name == null ? reduce.toString() : name, windowedOperator, windowedOperator.input, windowedOperator.output);
  }

  @Override
  public <K, V, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, V>>>> STREAM reduceByKey(String name, ReduceFn<V> reduce, Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(name != null ? name + "_premap" : convertToKeyVal.toString(), convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, V, V> keyedWindowedOperator = createKeyedWindowedOperator(reduce);
    return kvstream.addOperator(name == null ? reduce.toString() : name, keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output);
  }

  @Override
  public <K, V, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, V>>>> STREAM reduceByKey(ReduceFn<V> reduce, Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    return reduceByKey(null, reduce, convertToKeyVal);
  }

  @Override
  public <O, STREAM extends WindowedStream<Tuple.WindowedTuple<O>>> STREAM fold(FoldFn<T, O> fold)
  {
    return fold(null, fold);
  }

  @Override
  public <O, STREAM extends WindowedStream<Tuple.WindowedTuple<O>>> STREAM fold(String name, FoldFn<T, O> fold)
  {
    WindowedStream<Tuple<T>> innerstream = map(name != null ? name + "_premap" : name, new ConvertFn<T>());
    WindowedOperatorImpl<T, O, O> windowedOperator = createWindowedOperator(fold);
    return innerstream.addOperator(name == null ? fold.toString() : name, windowedOperator, windowedOperator.input, windowedOperator.output);
  }

  @Override
  public <K, V, O, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, O>>>> STREAM foldByKey(FoldFn<V, O> fold, Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    return foldByKey(null, fold, convertToKeyVal);
  }

  @Override
  public <K, V, O, STREAM extends WindowedStream<Tuple.WindowedTuple<KeyValPair<K, O>>>> STREAM foldByKey(String
      name, FoldFn<V, O> fold, Function.MapFunction<T, Tuple<KeyValPair<K, V>>> convertToKeyVal)
  {
    WindowedStream<Tuple<KeyValPair<K, V>>> kvstream = map(name != null ? name + "_premap" : convertToKeyVal.toString(), convertToKeyVal);
    KeyedWindowedOperatorImpl<K, V, O, O> keyedWindowedOperator = createKeyedWindowedOperator(fold);
    return kvstream.addOperator(name == null ? fold.toString() : name, keyedWindowedOperator, keyedWindowedOperator.input, keyedWindowedOperator.output);

  }

  @Override
  public <O, K, STREAM extends WindowedStream<KeyValPair<K, Iterable<O>>>> STREAM groupByKey(Function.MapFunction<T,
      KeyValPair<K, O>> convertToKeyVal)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <STREAM extends WindowedStream<Iterable<T>>> STREAM group()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM resetTrigger(TriggerOption option)
  {
    triggerOption = option;
    return (STREAM)this;
  }

  @Override
  public <STREAM extends WindowedStream<T>> STREAM resetAllowedLateness(Duration allowedLateness)
  {
    this.allowedLateness = allowedLateness;
    return (STREAM)this;
  }

  @Override
  protected <O> ApexStream<O> newStream(DagMeta graph, Brick<O> newBrick)
  {
    ApexWindowedStreamImpl<O> newstream = new ApexWindowedStreamImpl<>();
    newstream.graph = graph;
    newstream.lastBrick = newBrick;
    newstream.windowOption = this.windowOption;
    newstream.triggerOption = this.triggerOption;
    newstream.allowedLateness = this.allowedLateness;
    return newstream;
  }

  private <IN, ACCU, OUT> WindowedOperatorImpl<IN, ACCU, OUT> createWindowedOperator(Accumulation<IN, ACCU, OUT> accumulationFn)
  {
    WindowedOperatorImpl<IN, ACCU, OUT> windowedOperator = new WindowedOperatorImpl<>();
    //TODO use other default setting in the future
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<ACCU>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedStorage<OUT>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    if (windowOption != null) {
      windowedOperator.setWindowOption(windowOption);
    }
    if (triggerOption != null) {
      windowedOperator.setTriggerOption(triggerOption);
    }
    if (allowedLateness != null) {
      windowedOperator.setAllowedLateness(allowedLateness);
    }
    windowedOperator.setAccumulation(accumulationFn);
    return windowedOperator;
  }

  private <K, V, ACCU, OUT> KeyedWindowedOperatorImpl<K, V, ACCU, OUT> createKeyedWindowedOperator(Accumulation<V, ACCU, OUT> accumulationFn)
  {
    KeyedWindowedOperatorImpl<K, V, ACCU, OUT> keyedWindowedOperator = new KeyedWindowedOperatorImpl<>();

    //TODO use other default setting in the future
    keyedWindowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<K, ACCU>());
    keyedWindowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<K, OUT>());
    keyedWindowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    if (windowOption != null) {
      keyedWindowedOperator.setWindowOption(windowOption);
    }
    if (triggerOption != null) {
      keyedWindowedOperator.setTriggerOption(triggerOption);
    }
    if (allowedLateness != null) {
      keyedWindowedOperator.setAllowedLateness(allowedLateness);
    }

    keyedWindowedOperator.setAccumulation(accumulationFn);
    return keyedWindowedOperator;
  }

}
