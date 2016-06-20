package org.apache.apex.malhar.stream.window;

import java.util.Map;
import java.util.Set;

/**
 * Created by david on 6/20/16.
 */
public interface WindowedStorage<T> extends Iterable<Map.Entry<Window, T>>
{
  /**
   * Returns true if the storage contains this window
   *
   * @param window
   */
  boolean containsWindow(Window window);

  /**
   * Sets the data associated with the given window
   *
   * @param window
   * @param value
   */
  void put(Window window, T value);

  /**
   * Gets the value associated with the given window
   *
   * @param window
   * @return
   */
  T get(Window window);

  /**
   * Gets the windows in the storage that end before the given timestamp
   *
   * @param timestamp
   * @return
   */
  Set<Window> windowsEndBefore(long timestamp);

  /**
   * Removes all the data associated with the given window
   *
   * @param window
   */
  void remove(Window window);

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

  /**
   * Returns the iterable of the entries in the storage
   *
   * @return
   */
  Iterable<Map.Entry<Window, T>> entrySet();
}
