package org.apache.apex.malhar.stream.window.impl;

import org.apache.apex.malhar.stream.window.Window;
import org.apache.apex.malhar.stream.window.WindowedKeyedStorage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by david on 6/14/16.
 */
public class InMemoryWindowedKeyedStorage<K, V> extends InMemoryWindowedStorage<Map<K, V>> implements WindowedKeyedStorage<K, V>
{
  @Override
  public void put(Window window, K key, V value)
  {
    Map<K, V> kvMap;
    if (map.containsKey(window)) {
      kvMap = map.get(window);
    } else {
      kvMap = map.put(window, new HashMap<K, V>());
    }
    kvMap.put(key, value);
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet(Window window)
  {
    if (map.containsKey(window)) {
      return map.get(window).entrySet();
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public V get(Window window, K key)
  {
    if (map.containsKey(window)) {
      return map.get(window).get(key);
    } else {
      return null;
    }
  }

}
