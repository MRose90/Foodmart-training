package foodmart;


import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, SalesCKey, Text> {


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		// Sum weekend and weekday
		Double sales = Double.parseDouble(tokens[6])+Double.parseDouble(tokens[7]);
		String s = new DecimalFormat("#0.00").format(sales);
		context.write(new SalesCKey(tokens[0],tokens[1],Double.parseDouble(s)),new Text(s+","+tokens[2]+","+tokens[3]));
	}
}