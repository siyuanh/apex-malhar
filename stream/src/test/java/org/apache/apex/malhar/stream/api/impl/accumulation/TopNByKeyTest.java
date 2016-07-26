package org.apache.apex.malhar.stream.api.impl.accumulation;

import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.lib.util.KeyValPair;

/**
 * Unit test for TopNByKey accumulation
 */
public class TopNByKeyTest
{
  public static class NumGen extends BaseOperator implements InputOperator
  {

    private static int num;

    public static String[] keys = {"a", "b", "c", "d"};

    public static String[] names = {"A", "B", "C", "D", "E", "D", "F"};

    public transient DefaultOutputPort<KeyValPair<String, KeyValPair<String, Integer>>> output = new DefaultOutputPort<>();

    public void setup(Context.OperatorContext context)
    {
      num = 1;
    }


    @Override
    public void emitTuples()
    {
      while (num <= 10) {
        for (String key : keys) {
          for (String name : names) {
            KeyValPair<String, Integer> score = new KeyValPair<>(name, (int)(Math.random() * 100));
            output.emit(new KeyValPair<String, KeyValPair<String, Integer>>(key, score));
          }
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static class StraightMap implements Function.MapFunction<KeyValPair<String, KeyValPair<String, Integer>>, Tuple<KeyValPair<String, KeyValPair<String, Integer>>>>
  {
    @Override
    public Tuple<KeyValPair<String, KeyValPair<String, Integer>>> f(KeyValPair<String, KeyValPair<String, Integer>> input)
    {
      return new Tuple.PlainTuple<>(input);
    }
  }

  @Test
  public void TopNByKeyTest() throws Exception
  {
    WindowOption windowOption = new WindowOption.GlobalWindow();

    NumGen numGen = new NumGen();
    TopNByKey<String, Integer> topNByKey = new TopNByKey<>();
    topNByKey.setN(3);
    ApexStream<KeyValPair<String, KeyValPair<String, Integer>>> s = StreamFactory.fromInput(numGen, numGen.output);
    s.window(windowOption, new TriggerOption().withEarlyFiringsAtEvery(50)).accumulateByKey(topNByKey, new StraightMap()).print()
        .runEmbedded(false, 10000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return false;
          }
        });
  }
}
