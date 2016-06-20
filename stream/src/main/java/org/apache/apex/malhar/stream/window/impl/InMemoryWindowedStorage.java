package org.apache.apex.malhar.stream.window.impl;

import org.apache.apex.malhar.stream.window.Window;
import org.apache.apex.malhar.stream.window.WindowedStorage;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by david on 6/20/16.
 */
public class InMemoryWindowedStorage<T> implements WindowedStorage<T>
{
  protected final TreeMap<Window, T> map = new TreeMap<>(Window.DEFAULT_COMPARATOR);

  @Override
  public void put(Window window, T value)
  {
    map.put(window, value);
  }

  @Override
  public boolean containsWindow(Window window)
  {
    return map.containsKey(window);
  }

  @Override
  public T get(Window window)
  {
    return map.get(window);
  }

  @Override
  public Set<Window> windowsEndBefore(long timestamp)
  {
    Set<Window> result = new TreeSet<>(Window.DEFAULT_COMPARATOR);
    Window refWindow = new Window.TimeWindow(timestamp, 0);
    for (Map.Entry<Window, T> entry : map.headMap(refWindow, true).entrySet()) {
      Window w = entry.getKey();
      if (timestamp >= w.getBeginTimestamp() + w.getDurationMillis()) {
        result.add(w);
      }
    }
    return result;
  }

  @Override
  public void remove(Window window)
  {
    map.remove(window);
  }

  @Override
  public void removeUpTo(long timestamp)
  {
    for (Window w : windowsEndBefore(timestamp)) {
      map.remove(w);
    }
  }

  @Override
  public void migrateWindow(Window fromWindow, Window toWindow)
  {
    if (containsWindow(fromWindow)) {
      map.put(toWindow, map.remove(fromWindow));
    }
  }

  @Override
  public Iterable<Map.Entry<Window, T>> entrySet()
  {
    return map.entrySet();
  }

  @Override
  public Iterator<Map.Entry<Window, T>> iterator()
  {
    return map.entrySet().iterator();
  }
}
