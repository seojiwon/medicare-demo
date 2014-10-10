package com.hortonworks.demo;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import java.io.IOException;
import java.util.*;

public class SimUDF extends EvalFunc<DataBag> {

  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory mBagFactory = BagFactory.getInstance();

  @Override
  public DataBag exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0)
   		return null;
	
		try { 
			HashSet processed_npis = new HashSet();
   		DataBag bag = (DataBag)input.get(0);
   		DataBag output = mBagFactory.newDefaultBag();

   		Iterator it = bag.iterator();
    	while (it.hasNext()) {
    		Tuple t1 = (Tuple)it.next();
    		String npi1 = (String)t1.get(0);
    		DataBag cpt_vec1 = (DataBag)t1.get(1);
    		if (processed_npis.contains(npi1))
    			continue;
    		processed_npis.add(npi1);
    		Iterator it2 = bag.iterator();
    		while (it2.hasNext()) {
      		Tuple t2 = (Tuple)it2.next();
      		String npi2 = (String)t2.get(0);
      		if (npi1.compareTo(npi2) <= 0)
        		continue;
      		DataBag cpt_vec2 = (DataBag)t2.get(1);
      		double s = cosineSim(cpt_vec1, cpt_vec2);
//					System.out.println(cpt_vec1);
//					System.out.println(cpt_vec2);
//					System.out.println("sim = " + s);
      		if (s >= 0.7) {
						Tuple t = mTupleFactory.newTuple(2);
						t.set(0, npi1);
						t.set(1, npi2);
     				output.add(t);
					}
    		}
			}
			PigStatusReporter.getInstance().getCounter("medicare-UDF", "bags").increment(1);
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
      	float count = (float)t.get(1);
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
      	Integer inx = new Integer((int)t.get(0));
      	Float count = new Float((float)t.get(1));
      	v2map.put(inx, count);
   		}
    	
  		double v1_norm = norm(v1);
    	double v2_norm = norm(v2);
			double angle = 0.0;
    	it = v1.iterator();
    	while (it.hasNext()) {
      	Tuple t = (Tuple)it.next();
      	Integer inx = new Integer((int)t.get(0));
      	if (v2map.containsKey(inx)) {
      		Float count = (Float)t.get(1);
        	angle += (double)(count * (Float)v2map.get(inx));
				}
    	}
    	return ((double)angle / (v1_norm*v2_norm));
		} catch (Exception e) {
			throw new IOException("Caught exception computing norm ", e);
  	}
	}
}

