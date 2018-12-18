package foodmart;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SalesPartitioner extends Partitioner<SalesCKey, Text>{

	@Override
	public int getPartition(SalesCKey key, Text t, int numPartitions) {
		return Math.abs(key.getNatKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
