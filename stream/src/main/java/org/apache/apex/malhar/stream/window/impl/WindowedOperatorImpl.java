package org.apache.apex.malhar.stream.window.impl;

import org.apache.apex.malhar.stream.window.Accumulation;
import org.apache.apex.malhar.stream.window.TriggerOption;
import org.apache.apex.malhar.stream.window.Tuple;
import org.apache.apex.malhar.stream.window.Window;
import org.apache.apex.malhar.stream.window.WindowedStorage;

/**
 * Created by david on 6/21/16.
 */
public class WindowedOperatorImpl<InputT, AccumT, OutputT>
    extends AbstractWindowedOperator<InputT, AccumT, OutputT, WindowedStorage<AccumT>, Accumulation<InputT, AccumT, OutputT>>
{
  @Override
  public void accumulateTuple(Tuple.WindowedTuple<InputT> tuple)
  {
    for (Window window : tuple.getWindows()) {
      // process each window
      AccumT accum = dataStorage.get(window);
      if (accum == null) {
        accum = accumulation.defaultAccumulatedValue();
      }
      dataStorage.put(window, accumulation.accumulate(accum, tuple.getValue()));
    }
  }

  @Override
  public void fireNormalTrigger(Window window)
  {
    AccumT accumulatedValue = dataStorage.get(window);
    output.emit(new Tuple.WindowedTuple<>(window, window.getBeginTimestamp(), accumulation.getOutput(accumulatedValue)));
    if (retractionStorage != null) {
      retractionStorage.put(window, accumulatedValue);
    }
  }

  @Override
  public void fireRetractionTrigger(Window window)
  {
    if (triggerOption.getAccumulationMode() != TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      throw new UnsupportedOperationException();
    }
    AccumT accumulatedValue = retractionStorage.get(window);
    output.emit(new Tuple.WindowedTuple<>(window, window.getBeginTimestamp(), accumulation.getRetraction(accumulatedValue)));
  }
}
