package org.apache.apex.malhar.stream.sample.complete;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.db.jdbc.JdbcFieldInfo;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;

import static java.sql.Types.VARCHAR;
/**
 * A streaming Dataflow Example using BigQuery output.
 *
 * <p>This pipeline example reads lines of the input text file, splits each line
 * into individual words, capitalizes those words, and writes the output to
 * a BigQuery table.
 *
 * <p>The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Dataflow pipeline).
 * You can override them by using the {@literal --bigQueryDataset}, and {@literal --bigQueryTable}
 * options. If the BigQuery table do not exist, the example will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class StreamingWordExtract implements StreamingApplication
{
  private static int wordCount = 0;
  private static int entriesMapped = 0;
  
  public int getWordCount()
  {
    return wordCount;
  }
  
  public int getEntriesMapped()
  {
    return entriesMapped;
  }
  
  /**
   * A MapFunction that tokenizes lines of text into individual words.
   */
  public static class ExtractWords implements Function.FlatMapFunction<String, String>
  {
    @Override
    public Iterable<String> f(String input)
    {
      List<String> result = new ArrayList<>(Arrays.asList(input.split("[^a-zA-Z0-9']+")));
      wordCount += result.size();
      return result;
    }
  }
  
  
  /**
   * A MapFunction that uppercases a word.
   */
  public static class Uppercase implements Function.MapFunction<String, String>
  {
    @Override
    public String f(String input)
    {
      return input.toUpperCase();
    }
  }
  
  
  public static class EmptyStringFilter implements Function.FilterFunction<String>
  {
  
    @Override
    public Boolean f(String input)
    {
      return !input.isEmpty();
    }
  }
  
  
  public static class PojoMapper implements Function.MapFunction<String, Object>
  {
  
    @Override
    public Object f(String input)
    {
      PojoEvent pojo = new PojoEvent();
      pojo.setString_value(input);
      entriesMapped++;
      return pojo;
    }
  }
  
  private static List<JdbcFieldInfo> addFieldInfos()
  {
    List<JdbcFieldInfo> fieldInfos = new ArrayList<>();
    fieldInfos.add(new JdbcFieldInfo("STRING_VALUE", "string_value", JdbcFieldInfo.SupportType.STRING, VARCHAR));
    return fieldInfos;
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOInsertOutputOperator jdbcOutput = new JdbcPOJOInsertOutputOperator();
    jdbcOutput.setFieldInfos(addFieldInfos());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbcOutput.setStore(outputStore);
    jdbcOutput.setTablename("WordExtractTest");
    
    ApexStream<String> stream = StreamFactory.fromFolder("./src/test/resources/data");

    stream.flatMap(new ExtractWords()).filter(new EmptyStringFilter()).map(new Uppercase()).map(new PojoMapper())
        .addOperator("jdbcOutput",jdbcOutput, jdbcOutput.input, null);
    
    stream.populateDag(dag);
  }
}
