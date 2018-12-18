package foodmart;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, Text, Text> {


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		// check that promotions is not 0
		if (!tokens[3].equals("0")) {
			// day
			switch (tokens[5]) {
			case ("Monday"):
			case ("Tuesday"):
			case ("Wednesday"):
			case ("Thursday"):
			case ("Friday"):
				// prefix the weekday sales with 0 so they are recognizable in
				// the reducer without changing the overall value
				// year month region promotion sales
				context.write(new Text(
						tokens[7] + "," + tokens[6] + "," + tokens[0] + "," + tokens[3]),
						new Text("0" + tokens[4]));
				break;

			case ("Saturday"):
			case ("Sunday"):
				context.write(new Text(tokens[7] + "," + tokens[6] + "," + tokens[0] + "," + tokens[3]),
						new Text(tokens[4]));
				break;

			default:
				break;
			}
		}
	}
}