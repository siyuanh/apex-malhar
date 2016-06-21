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

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.classification.InterfaceStability;
import org.joda.time.Duration;

/**
 * This class describes the windowing scheme, which includes:
 *  - how windowing is done
 *  - how triggers are fired
 *  - the allowed lateness
 *  - the accumulation mode
 *
 *  This is used by both the high level API and by the WindowedOperator
 */
@InterfaceStability.Evolving
public abstract class WindowOption
{
  // TODO: We might not want to include the TriggerOption, the AccumulationMode and the AllowedLateness in the WindowOption since the input of the operator might already be a WindowedTuple from upstream. Need further discussion

  private TriggerOption triggerOption;

  private AccumulationMode accumulationMode = AccumulationMode.DISCARDING;

  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration allowedLateness;

  public enum AccumulationMode
  {
    DISCARDING,
    ACCUMULATING,
    ACCUMULATING_AND_RETRACTING
  }

  public static class GlobalWindow extends WindowOption
  {
  }

  public static class TimeWindows extends WindowOption
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    private final Duration duration;

    public TimeWindows(Duration duration)
    {
      this.duration = duration;
    }

    public Duration getDuration()
    {
      return duration;
    }

    public SlidingTimeWindows slideBy(Duration duration)
    {
      return new SlidingTimeWindows(this.duration, duration);
    }
  }

  public static class SlidingTimeWindows extends TimeWindows
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    private Duration slideByDuration;

    public SlidingTimeWindows(Duration size, Duration slideByDuration)
    {
      super(size);
      if (size.getMillis() % slideByDuration.getMillis() != 0) {
        throw new IllegalArgumentException("Window size must be divisible by the slide-by duration");
      }
      this.slideByDuration = slideByDuration;
    }

    public Duration getSlideByDuration()
    {
      return slideByDuration;
    }
  }

  public static class SessionWindows extends WindowOption
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    private Duration minGap;

    public SessionWindows(Duration minGap)
    {
      this.minGap = minGap;
    }

    public Duration getMinGap()
    {
      return minGap;
    }
  }

  // Pre-defined trigger emit tuple
  public WindowOption triggering(TriggerOption option)
  {
    triggerOption = option;
    return this;
  }

  public WindowOption discardingFiredPanes()
  {
    this.accumulationMode = AccumulationMode.DISCARDING;
    return this;
  }

  public WindowOption accumulatingFiredPanes()
  {
    this.accumulationMode = AccumulationMode.ACCUMULATING;
    return this;
  }

  public WindowOption accumulatingAndRetractingFiredPane()
  {
    this.accumulationMode = AccumulationMode.ACCUMULATING_AND_RETRACTING;
    return this;
  }

  public WindowOption withAllowedLateness(Duration duration)
  {
    this.allowedLateness = duration;
    return this;
  }

  public Duration getAllowedLateness()
  {
    return this.allowedLateness;
  }

  public AccumulationMode getAccumulationMode()
  {
    return this.accumulationMode;
  }

  public TriggerOption getTriggerOption()
  {
    return triggerOption;
  }
}
