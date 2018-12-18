package foodmart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, Text, Text, Text> {
	
	private Map<String, String> promoMap = new HashMap<String, String>();

	protected void setup(Context context) throws java.io.IOException, InterruptedException {

		File prodFile = new File("Promos");
		FileInputStream fis = new FileInputStream(prodFile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

		String line = reader.readLine();

		while (line != null) {
			String[] tokens = line.split(",");
			String promoID = tokens[0];
			//Set promoData to Promotion name
			String promoData = tokens[1];
			//Add Promotion Cost to promoData
			promoData = promoData + "," + new DecimalFormat("#0.00").format(Double.parseDouble(tokens[2]));
			promoMap.put(promoID, promoData);
			line = reader.readLine();
		}
		reader.close();

		if (promoMap.isEmpty()) {
			throw new IOException("Unable to load Product data.");
		}
	}

	protected void reduce(Text token, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double weekday = 0;
		double weekend = 0;
		for (Text v : values) {
			String val = v.toString();
			//check if it's a weekday (double check that the value isn't 0.XX)
			if (val.charAt(0) == '0' && val.charAt(1)!= '.') {
				weekday += Double.valueOf(val);
			} else {
				weekend += Double.valueOf(val);
			}
		}
		String[] tokens = token.toString().split(",");
		String output = "," + promoMap.get(tokens[3]);
		//Nice formatting to have the weekday and weekday sales only show 2 decimals.
		output = output+","+new DecimalFormat("#0.00").format(weekday)+","+new DecimalFormat("#0.00").format(weekend);
		context.write(new Text(token+output), null);
	}
}