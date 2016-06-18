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
package org.apache.apex.malhar.stream.window;

import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This interface describes what needs to be implemented for the operator that supports the Apache Beam model of
 * windowing and triggering
 */
@InterfaceStability.Evolving
public interface WindowedOperator<InputT, KeyT, AccumT, OutputT>
{

  /**
   * Sets the WindowOption of this operator
   *
   * @param windowOption
   */
  void setWindowOption(WindowOption windowOption);

  /**
   * Sets the accumulation, which basically tells the WindowedOperator what to do if a new tuple comes in and what
   * to put in the pane when a trigger is fired
   *
   * @param accumulation
   */
  void setAccumulation(Accumulation<InputT, AccumT, OutputT> accumulation);

  /**
   * This method sets the storage for the data for each window
   *
   * @param storageAgent
   */
  void setDataStorage(WindowedStorage<KeyT, AccumT> storageAgent);

  /**
   * This method sets the storage for the retraction data for each window. Only used when the accumulation mode is ACCUMULATING_AND_RETRACTING
   *
   * @param storageAgent
   */
  void setRetractionStorage(WindowedStorage<KeyT, AccumT> storageAgent);

  /**
   * This sets the function that extracts the timestamp from the input tuple
   *
   * @param timestampExtractor
   */
  void setTimestampExtractor(Function.MapFunction<InputT, Long> timestampExtractor);

  /**
   * This sets the function that extracts the key from the input tuple. If this is not set, assume that the
   * input is not keyed.
   *
   * @param keyExtractor
   */
  void setKeyExtractor(Function.MapFunction<InputT, KeyT> keyExtractor);

  /**
   * Assign window(s) for this input tuple
   *
   * @param input
   * @return
   */
  Tuple.WindowedTuple<InputT> getWindowedValue(InputT input);

  /**
   * This method returns whether the given timestamp is too late for processing.
   * The implementation of this operator should look at the allowed lateness in the WindowOption.
   * It should also call this function and if it returns true, it should drop the associated tuple.
   *
   * @param timestamp
   * @return
   */
  boolean isTooLate(long timestamp);

  /**
   * This method is supposed to drop the tuple because it has passed the allowed lateness. But an implementation
   * of this method has the chance to do something different (e.g. emit it to another port)
   *
   * @param input
   */
  void dropTuple(Tuple<InputT> input);

  /**
   * This method accumulates the incoming tuple (with the Accumulation interface)
   *
   * @param tuple
   */
  void accumulateTuple(Tuple.WindowedTuple<InputT> tuple);

  /**
   * This method should be called when the watermark for the given timestamp arrives
   * The implementation should retrieve all valid windows in its state that lies completely before this watermark,
   * and change the state of each of those windows. All tuples for those windows arriving after
   * the watermark will be considered late.
   *
   * @param watermark
   */
  void processWatermark(Tuple.WatermarkTuple<InputT> watermark);

  /**
   * This method fires the trigger for the given window, and possibly retraction trigger. The implementation should clear
   * the window data in the storage if the accumulation mode is DISCARDING
   *
   * @param window
   */
  void fireTrigger(Window window, WindowState windowState);

  /**
   * This method fires the retraction trigger for the given window. This should only be valid if the accumulation
   * mode is ACCUMULATING_AND_RETRACTING
   *
   * @param window
   */
  void fireRetractionTrigger(Window window);

  /**
   * This method clears the window data in the storage.
   *
   * @param window
   */
  void clearWindowData(Window window);

  /**
   * This method invalidates the given window. The scenarios of calling this method are:
   *  1. The window has passed the allowed lateness
   *  2. The window has been merged with another window to form a new window
   *
   * @param window
   */
  void invalidateWindow(Window window);


}
