package com.hortonworks.demo;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.counters.PigCounterHelper;

import java.io.IOException;
import java.util.*;

public class SimUDF extends EvalFunc<DataBag> {

  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory mBagFactory = BagFactory.getInstance();
  PigCounterHelper helper = new PigCounterHelper();

  @Override
  public DataBag exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0)
   		return null;
	
		try { 
   		DataBag point = (DataBag)input.get(0);
			double distanceRange = ((Number)input.get(1)).doubleValue();
			DataBag vectorBag = (DataBag)input.get(2);
   		
			helper.incrCounter("medicare", "partitions", 1L);
			helper.incrCounter("medicare", "sim-count", vectorBag.size());

			DataBag output = mBagFactory.newDefaultBag();

   		Iterator it = vectorBag.iterator();
    	while (it.hasNext()) {
    		Tuple t1 = (Tuple)it.next();
    		String npi = (String)t1.get(0);
    		DataBag pt = (DataBag)t1.get(1);
      	double s = cosineSim(point, pt);
					
      	if (s >= 1.0-distanceRange) {
					helper.incrCounter("medicare", "sim-count-above-threshold", 1L);
					output.add(mTupleFactory.newTuple(npi));
    		} else if (s < 0) {
					throw new Exception("similarity < 0");
				}
			}
  		return output;
		} catch (Exception e) {
			throw new IOException("Caught exception processing row ", e);
		}
  }

  private double norm(DataBag v) throws IOException{
		try {
  		double sum = 0.0;
    	Iterator it = v.iterator();
    	while (it.hasNext()) {
      	Tuple t = (Tuple)it.next();
      	double count = (double)t.get(1);
      	sum += (double)(count*count);
    	}
    	return(Math.sqrt(sum));
		} catch (Exception e) {
			throw new IOException("Caught exception computing norm ", e);
		}
	}
        
	private double cosineSim(DataBag v1, DataBag v2) throws IOException {
		try {
    	HashMap v2map = new HashMap();
    	Iterator it = v2.iterator();
    	while (it.hasNext()) {
      	Tuple t = (Tuple)it.next();
      	int inx = (int)t.get(0);
      	double count = (double)t.get(1);
      	v2map.put(inx, count);
   		}
    	
  		double v1_norm = norm(v1);
    	double v2_norm = norm(v2);
			double angle = 0.0;
    	it = v1.iterator();
    	while (it.hasNext()) {
      	Tuple t = (Tuple)it.next();
      	int inx = (int)t.get(0);
      	if (v2map.containsKey(inx)) {
      		double count = (double)t.get(1);
        	angle += (double)(count * (Double)v2map.get(inx));
				}
    	}
    	return ((double)angle / (v1_norm*v2_norm));
		} catch (Exception e) {
			throw new IOException("Caught exception computing norm ", e);
  	}
	}
}

