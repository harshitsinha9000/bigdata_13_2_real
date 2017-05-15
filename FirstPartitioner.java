

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<IntPair, Text> {
	public int getPartition(IntPair key, Text value, int numPartitions) {
		return key.getyear().hashCode() % numPartitions;
	}
}
