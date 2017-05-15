

import java.io.IOException;
//import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<IntPair, Text, IntPair, Text> {
	
	public void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	  int current = 0;
	  int topN = 2;
      for (Text value : values) {
  		if (current == topN) {
  			break;
  	   }
  		context.write(key, value);
  	    current++;
      }  	
  }
}