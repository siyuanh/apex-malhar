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
package org.apache.apex.malhar.lib.window;

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * This interface is for storing data for session windowed streams.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
@InterfaceStability.Evolving
public interface SessionWindowedStorage<K, V> extends WindowedKeyedStorage<K, V>
{
  /**
   * Given the key, the timestamp and the gap, gets the data that falls into timestamp +/- gap.
   * This is used for getting the entry the data given the timestamp belongs to, and for determining whether to merge
   * session windows.
   * This should only return at most two entries if sessions have been merged appropriately.
   *
   * @param key the key
   * @param timestamp the timestamp
   * @param gap
   * @return
   */
  Collection<Map.Entry<Window.SessionWindow<K>, V>> getSessionEntries(K key, long timestamp, long gap);
}
