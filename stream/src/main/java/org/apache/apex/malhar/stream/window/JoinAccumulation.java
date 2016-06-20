package org.apache.apex.malhar.stream.window;

/**
 * This is the interface for accumulation when joining multiple streams.
 */
public interface JoinAccumulation<InputT1, InputT2, InputT3, InputT4, InputT5, AccumT, OutputT> extends Accumulation<InputT1, AccumT, OutputT>
{
  /**
   * Accumulate the second input type to the accumulated value
   *
   * @param accumulatedValue
   * @param input
   * @return
   */
  AccumT accumulate2(AccumT accumulatedValue, InputT2 input);

  /**
   * Accumulate the third input type to the accumulated value
   *
   * @param accumulatedValue
   * @param input
   * @return
   */
  AccumT accumulate3(AccumT accumulatedValue, InputT3 input);

  /**
   * Accumulate the fourth input type to the accumulated value
   *
   * @param accumulatedValue
   * @param input
   * @return
   */
  AccumT accumulate4(AccumT accumulatedValue, InputT4 input);

  /**
   * Accumulate the fifth input type to the accumulated value
   *
   * @param accumulatedValue
   * @param input
   * @return
   */
  AccumT accumulate5(AccumT accumulatedValue, InputT5 input);

}
