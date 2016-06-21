package org.apache.apex.malhar.stream.window.sample.pi;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.stream.window.Accumulation;
import org.apache.apex.malhar.stream.window.TriggerOption;
import org.apache.apex.malhar.stream.window.Tuple;
import org.apache.apex.malhar.stream.window.WindowOption;
import org.apache.apex.malhar.stream.window.WindowState;
import org.apache.apex.malhar.stream.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.stream.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.stream.window.impl.WindowedOperatorImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Duration;

/**
 * Created by david on 6/20/16.
 */
public class Application implements StreamingApplication
{
  public static class RandomNumberPairGenerator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Tuple<MutablePair<Double, Double>>> output = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      Tuple.PlainTuple<MutablePair<Double, Double>> tuple = new Tuple.PlainTuple<>(new MutablePair<>(Math.random(), Math.random()));
      this.output.emit(tuple);
    }
  }

  public static class PiAccumulation implements Accumulation<MutablePair<Double, Double>, MutablePair<Long, Long>, Double>
  {
    @Override
    public MutablePair<Long, Long> defaultAccumulatedValue()
    {
      return new MutablePair<>(0L, 0L);
    }

    @Override
    public MutablePair<Long, Long> accumulate(MutablePair<Long, Long> accumulatedValue, MutablePair<Double, Double> input)
    {
      long first = accumulatedValue.getLeft();
      long second = accumulatedValue.getRight();
      if (input.getLeft() * input.getLeft() + input.getRight() * input.getRight() < 1) {
        first++;
      }
      second++;
      return new MutablePair<>(first, second);
    }

    @Override
    public MutablePair<Long, Long> merge(MutablePair<Long, Long> accumulatedValue1, MutablePair<Long, Long> accumulatedValue2)
    {
      return new MutablePair<>(accumulatedValue1.getLeft() + accumulatedValue2.getLeft(), accumulatedValue1.getRight() + accumulatedValue2.getRight());
    }

    @Override
    public Double getOutput(MutablePair<Long, Long> accumulatedValue)
    {
      return accumulatedValue.getRight() == 0 ? 0.0 : (((double)accumulatedValue.getLeft()) * 4 / accumulatedValue.getRight());
    }

    @Override
    public Double getRetraction(MutablePair<Long, Long> accumulatedValue)
    {
      return -getOutput(accumulatedValue);
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    RandomNumberPairGenerator inputOperator = new RandomNumberPairGenerator();
    WindowedOperatorImpl<MutablePair<Double, Double>, Void, MutablePair<Long, Long>, Double> windowedOperator = new WindowedOperatorImpl();
    Accumulation<MutablePair<Double, Double>, MutablePair<Long, Long>, Double> piAccumulation = new PiAccumulation();

    windowedOperator.setAccumulation(piAccumulation);
    windowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<Void, MutablePair<Long, Long>>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setWindowOption(new WindowOption.GlobalWindow().triggering(new TriggerOption().withEarlyFiringsAtEvery(Duration.millis(1000))).accumulatingFiredPanes());

    ConsoleOutputOperator outputOperator = new ConsoleOutputOperator();
    dag.addOperator("inputOperator", inputOperator);
    dag.addOperator("windowedOperator", windowedOperator);
    dag.addOperator("outputOperator", outputOperator);
    dag.addStream("input_windowed", inputOperator.output, windowedOperator.input);
    dag.addStream("windowed_output", windowedOperator.output, outputOperator.input);
  }

  public static void main(String[] args) throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run();
  }
}
