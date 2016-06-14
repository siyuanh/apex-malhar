package org.apache.apex.malhar.stream.window.impl;

import com.datatorrent.api.StreamCodec;
import org.apache.apex.malhar.stream.window.Window;
import org.apache.apex.malhar.stream.window.WindowedStorage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by david on 6/14/16.
 */
public class DefaultWindowedStorageImpl<K, V> implements WindowedStorage<K, V>
{
  protected TreeMap<Window, Map<K, V>> map = new TreeMap<>(Window.DEFAULT_COMPARATOR);

  @Override
  public <W extends Window> void setWindowCodec(StreamCodec<W> codec)
  {
    // do nothing
  }

  @Override
  public void setKeyCodec(StreamCodec<K> codec)
  {
    // do nothing
  }

  @Override
  public void setValueCodec(StreamCodec<V> codec)
  {
    // do nothing
  }

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
  public Set<Window> windowsIncluding(long timestamp)
  {
    Set<Window> result = new TreeSet<>(Window.DEFAULT_COMPARATOR);
    Window refWindow = new Window.TimeWindow(timestamp, 0);
    for (Map.Entry<Window, Map<K, V>> entry : map.headMap(refWindow, true).entrySet()) {
      Window w = entry.getKey();
      if (timestamp >= w.getBeginTimestamp() && timestamp < w.getBeginTimestamp() + w.getDurationMillis()) {
        result.add(w);
      }
    }
    return result;
  }

  @Override
  public Set<Window> windowsEndBefore(long timestamp)
  {
    Set<Window> result = new TreeSet<>(Window.DEFAULT_COMPARATOR);
    Window refWindow = new Window.TimeWindow(timestamp, 0);
    for (Map.Entry<Window, Map<K, V>> entry : map.headMap(refWindow, true).entrySet()) {
      Window w = entry.getKey();
      if (timestamp >= w.getBeginTimestamp() + w.getDurationMillis()) {
        result.add(w);
      }
    }
    return result;
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

  @Override
  public void remove(Window window)
  {
    map.remove(window);
  }

  @Override
  public void remove(Window window, K key)
  {
    if (map.containsKey(window)) {
      map.get(window).remove(key);
    }
  }

  @Override
  public void removeUpTo(long timestamp)
  {
    for (Window w : windowsEndBefore(timestamp)) {
      map.remove(w);
    }
  }
}
