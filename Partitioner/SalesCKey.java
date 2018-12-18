package foodmart;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SalesCKey implements WritableComparable<SalesCKey>{
	private String year;
	private String month;
	private double revenue;
	
	public SalesCKey(){
		
	}
	public SalesCKey(String y, String m, double r) {
		// TODO Auto-generated constructor stub
		year = y;
		month = m;
		revenue = r;
	}
	
	public String getYear(){
		return year;
	}
	public String getMonth(){
		return month;
	}
	public double getRevenue(){
		return revenue;
	}
	public Text getNatKey(){
		return new Text(year+","+month);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readUTF();
		month = in.readUTF();
		revenue = in.readDouble();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(year);
		out.writeUTF(month);
		out.writeDouble(revenue);
		
	}

	@Override
	public int compareTo(SalesCKey o) {
		int yCmp = this.getYear().compareTo(o.getYear());
		if(yCmp != 0){
			return yCmp;
		} else {
			int mCmp = this.getMonth().compareTo(o.getMonth());
			if (mCmp != 0){
				return mCmp;
			} else {
				return -1 * Double.compare(this.getRevenue(),o.getRevenue());
			}
		}
	}
}
