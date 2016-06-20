package org.apache.apex.malhar.stream.window.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.window.Accumulation;
import org.apache.apex.malhar.stream.window.SessionWindowedStorage;
import org.apache.apex.malhar.stream.window.Tuple;
import org.apache.apex.malhar.stream.window.WindowedKeyedStorage;
import org.apache.apex.malhar.stream.window.TriggerOption;
import org.apache.apex.malhar.stream.window.Window;
import org.apache.apex.malhar.stream.window.WindowOption;
import org.apache.apex.malhar.stream.window.WindowState;
import org.apache.apex.malhar.stream.window.WindowedOperator;
import org.apache.apex.malhar.stream.window.WindowedStorage;
import org.joda.time.Duration;


/**
 * Created by david on 6/13/16.
 */
public class WindowedOperatorImpl<InputT, KeyT, AccumT, OutputT>
    extends BaseOperator implements WindowedOperator<InputT, KeyT, AccumT, OutputT>
{

  private WindowOption windowOption;
  private Accumulation<InputT, AccumT, OutputT> accumulation;
  private WindowedKeyedStorage<KeyT, AccumT> dataStorage;
  private WindowedKeyedStorage<KeyT, AccumT> retractionStorage;
  private WindowedStorage<WindowState> windowStateMap;

  private Function.MapFunction<InputT, Long> timestampExtractor;
  private Function.MapFunction<InputT, KeyT> keyExtractor;
  private long currentWatermark;
  private boolean triggerAtWatermark;
  private long earlyTriggerCount;
  private long earlyTriggerMillis;
  private long lateTriggerCount;
  private long lateTriggerMillis;
  private long currentApexWindowId = -1;
  private long currentDerivedTimestamp;
  private long firstWindowMillis;
  private long windowWidthMillis;

  public transient DefaultInputPort<Tuple<InputT>> input = new DefaultInputPort<Tuple<InputT>>()
  {
    @Override
    public void process(Tuple<InputT> tuple)
    {
      processTuple(tuple);
    }
  };

  // TODO: multiple input ports for join operations

  public transient DefaultOutputPort<Tuple<OutputT>> output = new DefaultOutputPort<>();

  protected void processTuple(Tuple<InputT> tuple)
  {
    if (tuple instanceof Tuple.WatermarkTuple) {
      processWatermark((Tuple.WatermarkTuple<InputT>) tuple);
    } else {
      long timestamp = timestampExtractor.f(tuple.getValue());
      if (isTooLate(timestamp)) {
        dropTuple(tuple);
      } else {
        Tuple.WindowedTuple<InputT> windowedTuple = getWindowedValue(tuple);
        // do the accumulation
        accumulateTuple(windowedTuple);

        for (Window window : windowedTuple.getWindows()) {
          WindowState windowState = windowStateMap.get(window);
          windowState.tupleCount++;
          // process any count based triggers
          if (windowState.watermarkArrivalTime == -1) {
            // watermark has not arrived yet, check for early count based trigger
            if (earlyTriggerCount > 0 && (windowState.tupleCount % earlyTriggerCount) == 0) {
              fireTrigger(window, windowState);
            }
          } else {
            // watermark has arrived, check for late count based trigger
            if (lateTriggerCount > 0 && (windowState.tupleCount % lateTriggerCount) == 0) {
              fireTrigger(window, windowState);
            }
          }
        }
      }
    }
  }

  @Override
  public void setWindowOption(WindowOption windowOption)
  {
    this.windowOption = windowOption;
    TriggerOption triggerOption = this.windowOption.getTriggerOption();
    for (TriggerOption.Trigger trigger : triggerOption.getTriggerList()) {
      switch (trigger.getWatermarkOpt()) {
        case ON_TIME:
          triggerAtWatermark = true;
          break;
        case EARLY:
          if (trigger instanceof TriggerOption.TimeTrigger) {
            earlyTriggerMillis = ((TriggerOption.TimeTrigger) trigger).getDuration().getMillis();
          } else if (trigger instanceof TriggerOption.CountTrigger) {
            earlyTriggerCount = ((TriggerOption.CountTrigger)trigger).getCount();
          }
          break;
        case LATE:
          if (trigger instanceof TriggerOption.TimeTrigger) {
            lateTriggerMillis = ((TriggerOption.TimeTrigger) trigger).getDuration().getMillis();
          } else if (trigger instanceof TriggerOption.CountTrigger) {
            lateTriggerCount = ((TriggerOption.CountTrigger)trigger).getCount();
          }
          break;
      }
    }
  }

  @Override
  public void setAccumulation(Accumulation<InputT, AccumT, OutputT> accumulation)
  {
    this.accumulation = accumulation;
  }

  @Override
  public void setDataStorage(WindowedKeyedStorage<KeyT, AccumT> storageAgent)
  {
    this.dataStorage = storageAgent;
  }

  @Override
  public void setRetractionStorage(WindowedKeyedStorage<KeyT, AccumT> storageAgent)
  {
    this.retractionStorage = storageAgent;
  }

  @Override
  public void setWindowStateStorage(WindowedStorage<WindowState> storageAgent)
  {
    this.windowStateMap = storageAgent;
  }

  @Override
  public void setTimestampExtractor(Function.MapFunction<InputT, Long> timestampExtractor)
  {
    this.timestampExtractor = timestampExtractor;
  }

  @Override
  public void setKeyExtractor(Function.MapFunction<InputT, KeyT> keyExtractor)
  {
    this.keyExtractor = keyExtractor;
  }

  @Override
  public Tuple.WindowedTuple<InputT> getWindowedValue(Tuple<InputT> input)
  {
    Tuple.WindowedTuple<InputT> windowedTuple = new Tuple.WindowedTuple<>();
    windowedTuple.setTimestamp(extractTimestamp(input));
    assignWindows(windowedTuple.getWindows(), input);
    return windowedTuple;
  }

  private long extractTimestamp(Tuple<InputT> tuple)
  {
    if (timestampExtractor == null) {
      if (tuple instanceof Tuple.TimestampedTuple) {
        return ((Tuple.TimestampedTuple) tuple).getTimestamp();
      } else {
        throw new IllegalStateException("Cannot extract timestamp from tuple");
      }
    } else {
      return timestampExtractor.f(tuple.getValue());
    }
  }

  private KeyT extractKey(InputT input)
  {
    if (keyExtractor == null) {
      if (input instanceof KeyValPair) {
        return (KeyT)((KeyValPair) input).getKey();
      } else {
        return defaultKey();
      }
    } else {
      return keyExtractor.f(input);
    }
  }

  protected KeyT defaultKey()
  {
    return null;
  }

  private void assignWindows(List<Window> windows, Tuple<InputT> inputTuple)
  {
    if (windowOption instanceof WindowOption.GlobalWindow) {

      windows.add(Window.GLOBAL_WINDOW);

    } else {

      long timestamp = extractTimestamp(inputTuple);
      if (windowOption instanceof WindowOption.TimeWindows) {

        for (Window.TimeWindow window : getTimeWindowsForTimestamp(timestamp)) {
          if (!windowStateMap.containsWindow(window)) {
            windowStateMap.put(window, new WindowState());
          }
          windows.add(window);
        }

      } else if (windowOption instanceof WindowOption.SessionWindows) {

        WindowOption.SessionWindows sessionWindowOption = (WindowOption.SessionWindows)windowOption;
        SessionWindowedStorage<KeyT, AccumT> sessionStorage = (SessionWindowedStorage<KeyT, AccumT>)dataStorage;
        KeyT key = extractKey(inputTuple.getValue());
        Collection<Map.Entry<Window.SessionWindow, AccumT>> sessionEntries = sessionStorage.getSessionEntries(key, timestamp, sessionWindowOption.getMinGap().getMillis());
        switch (sessionEntries.size()) {
          case 0: {
            // There are no existing windows within the minimum gap. Create a new session window
            Window.SessionWindow<KeyT> sessionWindow = new Window.SessionWindow<>(key, timestamp, 1);
            windowStateMap.put(sessionWindow, new WindowState());
            windows.add(sessionWindow);
            break;
          }
          case 1: {
            // There is already one existing window within the minimum gap. See whether we need to extend the time of that window
            Map.Entry<Window.SessionWindow, AccumT> sessionWindowEntry = sessionEntries.iterator().next();
            Window.SessionWindow<KeyT> sessionWindow = sessionWindowEntry.getKey();
            if (sessionWindow.getBeginTimestamp() <= timestamp && timestamp < sessionWindow.getBeginTimestamp() + sessionWindow.getDurationMillis()) {
              // The session window already covers the event
              windows.add(sessionWindow);
            } else {
              // The session window does not cover the event but is within the min gap
              if (windowOption.getAccumulationMode() == WindowOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
                // fire a retraction trigger because the session window will be enlarged
                fireRetractionTrigger(sessionWindow);
              }
              // create a new session window that covers the timestamp
              long newBeginTimestamp = Math.min(sessionWindow.getBeginTimestamp(), timestamp);
              long newEndTimestamp = Math.max(sessionWindow.getBeginTimestamp() + sessionWindow.getDurationMillis(), timestamp + 1);
              Window.SessionWindow<KeyT> newSessionWindow =
                  new Window.SessionWindow<>(key, newBeginTimestamp, newEndTimestamp - newBeginTimestamp);
              windowStateMap.remove(sessionWindow);
              sessionStorage.migrateWindow(sessionWindow, newSessionWindow);
              windowStateMap.put(newSessionWindow, new WindowState());
            }
            break;
          }
          case 2: {
            // There are two windows that fall within the minimum gap of the timestamp. We need to merge the two windows
            Map.Entry<Window.SessionWindow, AccumT> sessionWindowEntry1 = sessionEntries.iterator().next();
            Map.Entry<Window.SessionWindow, AccumT> sessionWindowEntry2 = sessionEntries.iterator().next();
            Window.SessionWindow<KeyT> sessionWindow1 = sessionWindowEntry1.getKey();
            Window.SessionWindow<KeyT> sessionWindow2 = sessionWindowEntry2.getKey();
            AccumT sessionData1 = sessionWindowEntry1.getValue();
            AccumT sessionData2 = sessionWindowEntry1.getValue();
            if (windowOption.getAccumulationMode() == WindowOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
              // fire a retraction trigger because the two session windows will be merged to a new window
              fireRetractionTrigger(sessionWindow1);
              fireRetractionTrigger(sessionWindow2);
            }
            long newBeginTimestamp = Math.min(sessionWindow1.getBeginTimestamp(), sessionWindow2.getBeginTimestamp());
            long newEndTimestamp = Math.max(sessionWindow1.getBeginTimestamp() + sessionWindow1.getDurationMillis(),
                sessionWindow2.getBeginTimestamp() + sessionWindow2.getDurationMillis());

            Window.SessionWindow<KeyT> newSessionWindow = new Window.SessionWindow<>(key, newBeginTimestamp, newEndTimestamp - newBeginTimestamp);
            AccumT newSessionData = accumulation.merge(sessionData1, sessionData2);
            sessionStorage.remove(sessionWindow1);
            sessionStorage.remove(sessionWindow2);
            sessionStorage.put(newSessionWindow, key, newSessionData);
            break;
          }
          default:
            throw new IllegalStateException("There are more than two sessions matching one timestamp");
        }
      }
    }
  }

  /**
   * Returns the list of windows TimeWindows for the given timestamp.
   * If we are doing sliding windows, this will return multiple windows. Otherwise, only one window will be returned.
   * Note that this method does not apply to SessionWindows.
   *
   * @param timestamp
   * @return
   */
  private List<Window.TimeWindow> getTimeWindowsForTimestamp(long timestamp)
  {
    List<Window.TimeWindow> windows = new ArrayList<>();
    if (windowOption instanceof WindowOption.TimeWindows) {
      long durationMillis = ((WindowOption.TimeWindows) windowOption).getDuration().getMillis();
      long beginTimestamp = timestamp - timestamp % durationMillis;
      windows.add(new Window.TimeWindow(beginTimestamp, durationMillis));
      if (windowOption instanceof WindowOption.SlidingTimeWindows) {
        long slideBy = ((WindowOption.SlidingTimeWindows) windowOption).getSlideByDuration().getMillis();
        // add the sliding windows front and back
        // Note: this messes up the order of the window and we might want to revisit this if the order of the windows
        // matter
        for (long slideBeginTimestamp = beginTimestamp - slideBy;
             slideBeginTimestamp >= timestamp && timestamp > slideBeginTimestamp + durationMillis;
             slideBeginTimestamp -= slideBy) {
          windows.add(new Window.TimeWindow(slideBeginTimestamp, durationMillis));
        }
        for (long slideBeginTimestamp = beginTimestamp + slideBy;
             slideBeginTimestamp >= timestamp && timestamp > slideBeginTimestamp + durationMillis;
             slideBeginTimestamp += slideBy) {
          windows.add(new Window.TimeWindow(slideBeginTimestamp, durationMillis));
        }
      }
    } else {
      throw new IllegalStateException("Unexpected WindowOption");
    }
    return windows;
  }

  @Override
  public boolean isTooLate(long timestamp)
  {
    Duration allowedLateness = windowOption.getAllowedLateness();
    return timestamp < currentWatermark - allowedLateness.getMillis();
  }

  @Override
  public void dropTuple(Tuple<InputT> input)
  {
    // do nothing
  }

  @Override
  public void accumulateTuple(Tuple.WindowedTuple<InputT> tuple)
  {
    KeyT key = keyExtractor.f(tuple.getValue());
    for (Window window : tuple.getWindows()) {
      // process each window
      AccumT accum = dataStorage.get(window, key);
      dataStorage.put(window, key, accumulation.accumulate(accum, tuple.getValue()));
    }
  }

  @Override
  public void processWatermark(Tuple.WatermarkTuple<InputT> watermark)
  {
    currentWatermark = watermark.getTimestamp();
    long horizon = currentWatermark - windowOption.getAllowedLateness().getMillis();
    // purge window that are too late to accept any more input
    dataStorage.removeUpTo(horizon);

    for (Iterator<Map.Entry<Window, WindowState>> it = windowStateMap.iterator(); it.hasNext(); ) {
      Map.Entry<Window, WindowState> entry = it.next();
      Window window = entry.getKey();
      WindowState windowState = entry.getValue();
      if (window.getBeginTimestamp() + window.getDurationMillis() < horizon) {
        // discard this window because it's too late now
        it.remove();
      } else if (window.getBeginTimestamp() + window.getDurationMillis() < currentWatermark) {
        // watermark has not arrived for this window before, marking this window late
        if (windowState.watermarkArrivalTime == -1) {
          windowState.watermarkArrivalTime = currentDerivedTimestamp;
          if (triggerAtWatermark) {
            // fire trigger at watermark if applicable
            fireTrigger(window, windowState);
          }
        }
      }
    }

    output.emit((Tuple.WatermarkTuple<OutputT>)watermark);

    // TODO: if join operation (multiple input ports), we need to keep track of the watermark on all ports, and then propagate the watermark only when a new watermark arrives with the smallest timestamp among all the ports
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (this.firstWindowMillis <= 0) {
      this.firstWindowMillis = System.currentTimeMillis();
    }
    this.windowWidthMillis = context.getValue(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS);
  }

  /**
   * This is for the Apex streaming/application window. Do not confuse this with the windowing concept in this operator
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.currentApexWindowId = windowId;
    this.currentDerivedTimestamp = WindowGenerator.getWindowMillis(currentApexWindowId, firstWindowMillis, windowWidthMillis);
  }

  /**
   * This is for the Apex streaming/application window. Do not confuse this with the windowing concept in this operator
   */
  @Override
  public void endWindow()
  {
    fireTimeTriggers();
  }

  private void fireTimeTriggers()
  {
    if (earlyTriggerMillis > 0 || lateTriggerMillis > 0) {
      for (Map.Entry<Window, WindowState> entry : windowStateMap.entrySet()) {
        Window window = entry.getKey();
        WindowState windowState = entry.getValue();
        if (windowState.watermarkArrivalTime == -1) {
          if (earlyTriggerMillis > 0 && windowState.lastTriggerFiredTime + earlyTriggerMillis <= currentDerivedTimestamp) {
            // fire early time triggers
            fireTrigger(window, windowState);
          }
        } else {
          if (lateTriggerMillis > 0 && windowState.lastTriggerFiredTime + lateTriggerMillis <= currentDerivedTimestamp) {
            // fire late time triggers
            fireTrigger(window, windowState);
          }
        }
      }
    }
  }

  @Override
  public void fireTrigger(Window window, WindowState windowState)
  {
    if (windowOption.getAccumulationMode() == WindowOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      fireRetractionTrigger(window);
    }
    for (Map.Entry<KeyT, AccumT> entry : dataStorage.entrySet(window)) {
      output.emit(new Tuple.WindowedTuple<>(window, window.getBeginTimestamp(), accumulation.getOutput(entry.getValue())));
      if (retractionStorage != null) {
        retractionStorage.put(window, entry.getKey(), entry.getValue());
      }
    }
    windowState.lastTriggerFiredTime = WindowGenerator.getWindowMillis(currentApexWindowId, firstWindowMillis, windowWidthMillis);;
    windowState.lastTriggerFiredTupleCount = windowState.tupleCount;
    if (windowOption.getAccumulationMode() == WindowOption.AccumulationMode.DISCARDING) {
      clearWindowData(window);
    }
  }

  @Override
  public void fireRetractionTrigger(Window window)
  {
    if (windowOption.getAccumulationMode() != WindowOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      throw new UnsupportedOperationException();
    }
    for (Map.Entry<KeyT, AccumT> entry : retractionStorage.entrySet(window)) {
      output.emit(new Tuple.WindowedTuple<>(window, window.getBeginTimestamp(), accumulation.getRetraction(entry.getValue())));
    }
  }

  @Override
  public void clearWindowData(Window window)
  {
    dataStorage.remove(window);
  }

  @Override
  public void invalidateWindow(Window window)
  {
    dataStorage.remove(window);
    if (retractionStorage != null) {
      retractionStorage.remove(window);
    }
    windowStateMap.remove(window);
  }
}