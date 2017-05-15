

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, IntPair, Text> {
    private IntPair compositeKey = new IntPair();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

      String[] lineArray = line.split(",");
      String year = lineArray[0].split("-")[2];
      String temp = lineArray[2];
      
      compositeKey.set(Integer.parseInt(year), Integer.parseInt(temp));
      context.write(compositeKey, value);
  }
}