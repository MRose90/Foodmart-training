package foodmart;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<SalesCKey, Text, Text, Text> {

	protected void reduce(SalesCKey token, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (Text v : values) {
			if(count < 3){
				String[] vals = v.toString().split(",");
				context.write(new Text(token.getNatKey() + "," + vals[1]+","+vals[2]+","+vals[0]), null);
			} 
			count ++;
		}
	}
}
