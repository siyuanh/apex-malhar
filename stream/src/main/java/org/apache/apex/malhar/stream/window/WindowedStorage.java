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

import com.datatorrent.api.StreamCodec;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Map;
import java.util.Set;

/**
 * This interface is for a key/value store for storing data for windowed streams.
 * The key to this key/value store is a pair of (Window, K)
 *
 * Note that this interface does not take recovery into consideration.
 *
 */
@InterfaceStability.Evolving
public interface WindowedStorage<K, V>
{
  /**
   * Sets the codec (serializer and deserializer) for W. This will be used when storing and retrieving the window to and
   * from the storage. In most cases, the starting epoch millis should be used for TimedWindow and the starting tuple
   * offset should be used for CountWindow
   *
   * @param codec
   */
  void setWindowCodec(StreamCodec<Window> codec);

  /**
   * Sets the codec (serializer and deserializer) for K. This will be used when storing and retrieving the key to and
   * from the storage
   *
   * @param codec
   */
  void setKeyCodec(StreamCodec<K> codec);

  /**
   * Sets the codec (serializer and deserializer) for V. This will be used when storing and retrieving the value to and
   * from the storage
   *
   * @param codec
   */
  void setValueCodec(StreamCodec<V> codec);

  /**
   * Sets the data associated with the given window and the key
   *
   * @param window
   * @param key
   * @param value
   */
  void put(Window window, K key, V value);

  /**
   * Gets the key/value pairs associated with the given window
   *
   * @param window
   * @return
   */
  Set<Map.Entry<K, V>> entrySet(Window window);

  /**
   * Gets the windows in the storage for a given timestamp
   *
   * @param timestamp
   */
  Set<Window> windowsIncluding(long timestamp);

  /**
   * Gets the windows in the storage that end before the given timestamp
   *
   * @param timestamp
   * @return
   */
  Set<Window> windowsEndBefore(long timestamp);

  /**
   * Gets the data associated with the given window and the key
   *
   * @param window
   * @param key
   * @return
   */
  V get(Window window, K key);

  /**
   * Removes all the data associated with the given window
   *
   * @param window
   */
  void remove(Window window);

  /**
   * Removes the data associated with the given window and the key
   *
   * @param window
   * @param key
   */
  void remove(Window window, K key);

  /**
   * Removes all data in this storage that is associated with a window at or before the specified timestamp.
   * This will be used for purging data beyond the allowed lateness
   *
   * @param timestamp
   */
  void removeUpTo(long timestamp);

  /**
   * Migrate the data from one window to another. This will invalidate fromWindow in the storage and move the
   * data to toWindow, and overwrite any existing data in toWindow
   *
   * @param fromWindow
   * @param toWindow
   */
  void migrateWindow(Window fromWindow, Window toWindow);
}
