package socialite.tables;

import gnu.trove.iterator.TDoubleObjectIterator;
import gnu.trove.iterator.TFloatObjectIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TDoubleObjectHashMap;
import gnu.trove.map.hash.TFloatObjectHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.HashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import socialite.tables.ChunkPos;
import socialite.util.Assert;
import socialite.util.SociaLiteException;
import socialite.util.concurrent.*;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import socialite.visitors.IntVisitor;
import socialite.visitors.ObjectVisitor;

public final class SPosIndex {
	public static final Log L=LogFactory.getLog(SPosIndex.class);
	
	Object index;

	public SPosIndex() {}

	public void clear() {
		if (index==null) return;

        if (index instanceof TIntObjectHashMap) iIndex().clear();
        else if (index instanceof TLongObjectHashMap) lIndex().clear();
        else if (index instanceof TFloatObjectHashMap) fIndex().clear();
        else if (index instanceof TDoubleObjectHashMap) dIndex().clear();
        else if (index instanceof HashMap) oIndex().clear(); 
        else {
            throw new AssertionError("Unexpected index type:"+index.getClass().getSimpleName());
        }
	}
    
	void visitPos(Object val, IntVisitor v) {
		if (val == null) {
            return;
		} else if (val instanceof Integer) {
            v.visit((Integer)val);
        } else {
            MyIndexPosList posList = (MyIndexPosList)val;
            posList.iterate(v);
        }
	}
	
    // int-type key
	@SuppressWarnings({ "unchecked", "rawtypes" })
	TIntObjectHashMap iIndex() {
		if (index == null) {
            synchronized (this) {
                if (index == null) { index = new TIntObjectHashMap(); }
            }
        }
		return (TIntObjectHashMap)index;
	}

	public void add(int key, int pos) {
        Object val = iIndex().get(key);
		if (val == null) {
            iIndex().put(key, pos);
		} else if (val instanceof Integer) {
            MyIndexPosList posList = new  MyIndexPosList();
            posList.add((Integer)val);
            posList.add(pos);
            iIndex().put(key, posList);
        } else {
            MyIndexPosList posList = (MyIndexPosList)val;
            posList.add(pos);
        }
	}
    public Object get(int key) { return iIndex().get(key); }
    public void iterateBy(int key, IntVisitor v) {
        Object val = iIndex().get(key);
        visitPos(val, v);
    }
    public void iterateFrom(int from, boolean fromInclusive, IntVisitor v) {
    	TIntObjectIterator iter = iIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		int k = iter.key();
    		if (fromInclusive && k < from) continue;
    		if (!fromInclusive && k <= from) continue;
    		visitPos(iter.value(), v);
    	}
    }
    public void iterateTo(int to, boolean toInclusive, IntVisitor v) {
    	TIntObjectIterator iter = iIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		int k = iter.key();
    		if (toInclusive && k > to) continue;
    		if (!toInclusive && k >= to) continue;
    		visitPos(iter.value(), v);
    	}
    }
    public void iterateFromTo(int from, boolean fromInclusive, int to, boolean toInclusive, IntVisitor v) {
    	TIntObjectIterator iter = iIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		int k = iter.key();
    		if (fromInclusive && k < from) continue;
    		if (!fromInclusive && k <= from) continue;
    		if (toInclusive && k > to) continue;
    		if (!toInclusive && k >= to) continue;
    		visitPos(iter.value(), v);
    	}
    }
    
    // long-type key
	@SuppressWarnings({ "unchecked", "rawtypes" })
	TLongObjectHashMap lIndex() {
		if (index == null) {
            synchronized (this) {
                if (index == null) { index = new TLongObjectHashMap(); }
            }
        }
		return (TLongObjectHashMap)index;
	}
	public void add(long key, int pos) {
        Object val = lIndex().get(key);
		if (val == null) {
            lIndex().put(key, pos);
		} else if (val instanceof Integer) {
            MyIndexPosList posList = new  MyIndexPosList();
            posList.add((Integer)val);
            posList.add(pos);
            lIndex().put(key, posList);
        } else {
            MyIndexPosList posList = (MyIndexPosList)val;
            posList.add(pos);
        }
	}
    public Object get(long key) { return lIndex().get(key); }
    public void iterateBy(long key, IntVisitor v) {
        Object val = lIndex().get(key);
        visitPos(val, v);
    }
    public void iterateFrom(long from, boolean fromInclusive, IntVisitor v) {
    	TLongObjectIterator iter = lIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		long k = iter.key();
    		if (fromInclusive && k < from) continue;
    		if (!fromInclusive && k <= from) continue;
    		visitPos(iter.value(), v);
    	}
    }
    public void iterateTo(long to, boolean toInclusive, IntVisitor v) {
    	TLongObjectIterator iter = lIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		long k = iter.key();
    		if (toInclusive && k > to) continue;
    		if (!toInclusive && k >= to) continue;
    		visitPos(iter.value(), v);
    	}
    }
    public void iterateFromTo(long from, boolean fromInclusive, long to, boolean toInclusive, IntVisitor v) {
    	TLongObjectIterator iter = lIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		long k = iter.key();
    		if (fromInclusive && k < from) continue;
    		if (!fromInclusive && k <= from) continue;
    		if (toInclusive && k > to) continue;
    		if (!toInclusive && k >= to) continue;
    		visitPos(iter.value(), v);
    	}
    }
    
    // float-type key
	@SuppressWarnings({ "unchecked", "rawtypes" })
	TFloatObjectHashMap fIndex() {
		if (index == null) {
            synchronized (this) {
                if (index == null) { index = new TFloatObjectHashMap(); }
            }
        }
		return (TFloatObjectHashMap)index;
	}
	public void add(float key, int pos) {
        Object val = fIndex().get(key);
		if (val == null) {
            fIndex().put(key, pos);
		} else if (val instanceof Integer) {
            MyIndexPosList posList = new  MyIndexPosList();
            posList.add((Integer)val);
            posList.add(pos);
            fIndex().put(key, posList);
        } else {
            MyIndexPosList posList = (MyIndexPosList)val;
            posList.add(pos);
        }
	}
    public Object get(float key) { return fIndex().get(key); }
    public void iterateBy(float key, IntVisitor v) {
        Object val = fIndex().get(key);
        visitPos(val, v);
    }
    public void iterateFrom(float from, boolean fromInclusive, IntVisitor v) {
    	TFloatObjectIterator iter = fIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		float k = iter.key();
    		if (fromInclusive && k < from) continue;
    		if (!fromInclusive && k <= from) continue;
    		visitPos(iter.value(), v);
    	}
    }
    public void iterateTo(float to, boolean toInclusive, IntVisitor v) {
    	TFloatObjectIterator iter = fIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		float k = iter.key();
    		if (toInclusive && k > to) continue;
    		if (!toInclusive && k >= to) continue;
    		visitPos(iter.value(), v);
    	}
    }
    public void iterateFromTo(float from, boolean fromInclusive, float to, boolean toInclusive, IntVisitor v) {
    	TFloatObjectIterator iter = fIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		float k = iter.key();
    		if (fromInclusive && k < from) continue;
    		if (!fromInclusive && k <= from) continue;
    		if (toInclusive && k > to) continue;
    		if (!toInclusive && k >= to) continue;
    		visitPos(iter.value(), v);
    	}
    }
    
    // double-type key
	@SuppressWarnings({ "unchecked", "rawtypes" })
	TDoubleObjectHashMap dIndex() {
		if (index == null) {
            synchronized (this) {
                if (index == null) { index = new TDoubleObjectHashMap(); }
            }
        }
		return (TDoubleObjectHashMap)index;
	}
	public void add(double key, int pos) {
        Object val = dIndex().get(key);
		if (val == null) {
            dIndex().put(key, pos);
		} else if (val instanceof Integer) {
            MyIndexPosList posList = new  MyIndexPosList();
            posList.add((Integer)val);
            posList.add(pos);
            dIndex().put(key, posList);
        } else {
            MyIndexPosList posList = (MyIndexPosList)val;
            posList.add(pos);
        }
	}
    public Object get(double key) { return dIndex().get(key); }
    public void iterateBy(double key, IntVisitor v) {
        Object val = dIndex().get(key);
        visitPos(val, v);
    }
    public void iterateFrom(double from, boolean fromInclusive, IntVisitor v) {
    	TDoubleObjectIterator iter = dIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		double k = iter.key();
    		if (fromInclusive && k < from) continue;
    		if (!fromInclusive && k <= from) continue;
    		visitPos(iter.value(), v);
    	}
    }
    public void iterateTo(double to, boolean toInclusive, IntVisitor v) {
    	TDoubleObjectIterator iter = dIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		double k = iter.key();
    		if (toInclusive && k > to) continue;
    		if (!toInclusive && k >= to) continue;
    		visitPos(iter.value(), v);
    	}
    }
    public void iterateFromTo(double from, boolean fromInclusive, double to, boolean toInclusive, IntVisitor v) {
    	TDoubleObjectIterator iter = dIndex().iterator();
    	while (iter.hasNext()) {
    		iter.advance();
    		double k = iter.key();
    		if (fromInclusive && k < from) continue;
    		if (!fromInclusive && k <= from) continue;
    		if (toInclusive && k > to) continue;
    		if (!toInclusive && k >= to) continue;
    		visitPos(iter.value(), v);
    	}
    }
    
    HashMap oIndex() {
    	if (index == null) {
    		synchronized (this) {
    			if (index == null) { index = new HashMap(); }
    		}
    	}
    	return (HashMap)index;
    }
    public void add(Object key, int pos) {
    	Object val = oIndex().get(key);
		if (val == null) {
            oIndex().put(key, pos);
		} else if (val instanceof Integer) {
            MyIndexPosList posList = new  MyIndexPosList();
            posList.add((Integer)val);
            posList.add(pos);
            oIndex().put(key, posList);
        } else {
            MyIndexPosList posList = (MyIndexPosList)val;
            posList.add(pos);
        }
    }
    public Object get(Object key) { return oIndex().get(key); }
    public void iterateBy(Object key, IntVisitor v) {
    	Object val = oIndex().get(key);
    	visitPos(val, v);
    }
    public void iterateFrom(Object _from, boolean fromInclusive, IntVisitor v) {
    	if (!(_from instanceof Comparable)) {
    		L.error("Object "+_from+" is not Comparable type.");
    		throw new SociaLiteException("Cannot convert to Comparable type.");
    	}
    	Comparable from = (Comparable)_from;
    	for (Object _k:oIndex().keySet()) {
    		Comparable k = (Comparable)_k;
    		if (fromInclusive && k.compareTo(from)<0) continue;
    		if (!fromInclusive && k.compareTo(from)<=0) continue;
    		Object val = oIndex().get(_k);
    		visitPos(val, v);
    	}
    }
    public void iterateTo(Object _to, boolean toInclusive, IntVisitor v) {
    	if (!(_to instanceof Comparable)) {
    		L.error("Object "+_to+" is not Comparable type.");
    		throw new SociaLiteException("Cannot convert to Comparable type.");
    	}
    	Comparable to = (Comparable)_to;
    	for (Object _k:oIndex().keySet()) {
    		Comparable k = (Comparable)_k;
    		if (toInclusive && k.compareTo(to)>0) continue;
    		if (!toInclusive && k.compareTo(to)>=0) continue;
    		Object val = oIndex().get(_k);
    		visitPos(val, v);
    	}
    }
    public void iterateFromTo(Object _from, boolean fromInclusive, Object _to, boolean toInclusive, IntVisitor v) {
    	if (!(_from instanceof Comparable) || !(_to instanceof Comparable)) {
    		L.error("Object "+_from+" or "+_to+" is not Comparable type.");
    		throw new SociaLiteException("Cannot convert to Comparable type.");
    	}
    	Comparable from = (Comparable)_from;
    	Comparable to = (Comparable)_to;
    	for (Object _k:oIndex().keySet()) {
    		Comparable k = (Comparable)_k;
    		if (fromInclusive && k.compareTo(from)<0) continue;
    		if (!fromInclusive && k.compareTo(from)<=0) continue;
    		if (toInclusive && k.compareTo(to)>0) continue;
    		if (!toInclusive && k.compareTo(to)>=0) continue;
    		Object val = oIndex().get(_k);
    		visitPos(val, v);
    	}
    }
}