package foodmart;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SalesComparator extends WritableComparator{
	public SalesComparator(){
		super(SalesCKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		SalesCKey k1 = (SalesCKey) w1;
		SalesCKey k2 = (SalesCKey) w2;
		return k1.getNatKey().compareTo(k2.getNatKey());
	}
}
