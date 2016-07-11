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

import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * This interface describes the individual window.
 */
@InterfaceStability.Evolving
public interface Window
{
  long getBeginTimestamp();

  long getDurationMillis();

  /**
   * Global window means there is only one window, or no window depending on how you look at it.
   */
  class GlobalWindow implements Window
  {
    private GlobalWindow()
    {
    }

    @Override
    public long getBeginTimestamp()
    {
      return 0;
    }

    public long getDurationMillis()
    {
      return Long.MAX_VALUE;
    }

    @Override
    public boolean equals(Object other)
    {
      return (other instanceof GlobalWindow);
    }
  }

  class DefaultComparator implements Comparator<Window>
  {
    private DefaultComparator()
    {
    }

    @Override
    public int compare(Window o1, Window o2)
    {
      if (o1.getBeginTimestamp() < o2.getBeginTimestamp()) {
        return -1;
      } else if (o1.getBeginTimestamp() > o2.getBeginTimestamp()) {
        return 1;
      } else if (o1.getDurationMillis() < o2.getDurationMillis()) {
        return -1;
      } else if (o1.getDurationMillis() > o2.getDurationMillis()) {
        return 1;
      } else if (o1 instanceof SessionWindow && o2 instanceof SessionWindow) {
        return Long.compare(((SessionWindow)o1).getKey().hashCode(), ((SessionWindow)o2).getKey().hashCode());
      } else {
        return 0;
      }
    }
  }

  /**
   * The singleton global window
   */
  GlobalWindow GLOBAL_WINDOW = new GlobalWindow();

  /**
   * The singleton default comparator of windows
   */
  Comparator<Window> DEFAULT_COMPARATOR = new DefaultComparator();

  /**
   * TimeWindow is a window that represents a time slice
   */
  class TimeWindow implements Window
  {
    protected long beginTimestamp;
    protected long durationMillis;

    private TimeWindow()
    {
      // for kryo
    }

    public TimeWindow(long beginTimestamp, long durationMillis)
    {
      this.beginTimestamp = beginTimestamp;
      this.durationMillis = durationMillis;
    }

    /**
     * Gets the beginning timestamp of this window
     *
     * @return
     */
    @Override
    public long getBeginTimestamp()
    {
      return beginTimestamp;
    }

    /**
     * Gets the duration millis of this window
     *
     * @return
     */
    @Override
    public long getDurationMillis()
    {
      return durationMillis;
    }

    @Override
    public boolean equals(Object other)
    {
      if (other instanceof TimeWindow) {
        TimeWindow otherWindow = (TimeWindow)other;
        return this.beginTimestamp == otherWindow.beginTimestamp && this.durationMillis == otherWindow.durationMillis;
      } else {
        return false;
      }
    }

  }

  /**
   * SessionWindow is a window that represents a time slice for a key, with the time slice being variable length.
   *
   * @param <K>
   */
  class SessionWindow<K> extends TimeWindow
  {
    private K key;

    private SessionWindow()
    {
      // for kryo
    }

    public SessionWindow(K key, long beginTimestamp, long duration)
    {
      super(beginTimestamp, duration);
      this.key = key;
    }

    public K getKey()
    {
      return key;
    }

    @Override
    public boolean equals(Object other)
    {
      if (!super.equals(other)) {
        return false;
      }
      if (other instanceof SessionWindow) {
        SessionWindow<K> otherSessionWindow = (SessionWindow<K>)other;
        if (key == null) {
          return otherSessionWindow.key == null;
        } else {
          return key.equals(otherSessionWindow.key);
        }
      } else {
        return false;
      }
    }
  }
}
