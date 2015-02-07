package socialite.tables;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;

import gnu.trove.map.hash.*;
import socialite.collection.SDoubleIntHashMap;
import socialite.collection.SFloatIntHashMap;
import socialite.collection.SIntIntHashMap;
import socialite.collection.SLongIntHashMap;
import socialite.collection.SObjectIntHashMap;
import socialite.util.Assert;

public class GroupbyMap {
	Object map;
	int initCapacity=2048;
		
	public GroupbyMap() { }
	public GroupbyMap(int _initCapacity) { initCapacity=_initCapacity; }
	
	SIntIntHashMap intIntMap() {
		if (map==null) {map = (Object)new SIntIntHashMap(initCapacity, 0.8f, Integer.MIN_VALUE, -1);}
		return (SIntIntHashMap)map;
	}
	SLongIntHashMap longIntMap() {
		if (map==null) {map = (Object)new SLongIntHashMap(initCapacity, 0.8f, Long.MIN_VALUE, -1);}
		return (SLongIntHashMap)map;		
	}
	SFloatIntHashMap floatIntMap() {
		if (map==null) {map = (Object)new SFloatIntHashMap(initCapacity, 0.8f, Float.MIN_VALUE, -1);}
		return (SFloatIntHashMap)map;
	}
	SDoubleIntHashMap doubleIntMap() {
		if (map==null) {map = (Object)new SDoubleIntHashMap(initCapacity, 0.8f, Double.MIN_VALUE, -1);}
		return (SDoubleIntHashMap)map;
	}
	@SuppressWarnings("unchecked")
	SObjectIntHashMap<Object> objIntMap() {
		if (map==null) {map = (Object)new SObjectIntHashMap<Object>(initCapacity, 0.8f, -1);}
		return (SObjectIntHashMap<Object>)map;
	}
	
	@SuppressWarnings("unchecked")
	SObjectIntHashMap<Tuple> tupIntMap() {
		if (map==null) {map = (Object)new SObjectIntHashMap<Tuple>(initCapacity, 0.8f, -1);}
		return (SObjectIntHashMap<Tuple>)map;
	}

    TIntObjectHashMap<ChunkPos> intChunkPosMap() {
        if (map==null) {map = (Object)new TIntObjectHashMap(initCapacity, 0.8f, Integer.MIN_VALUE);}
        return (TIntObjectHashMap<ChunkPos>)map;
    }
    TLongObjectHashMap<ChunkPos> longChunkPosMap() {
        if (map==null) {map = (Object)new TLongObjectHashMap(initCapacity, 0.8f, Long.MIN_VALUE);}
        return (TLongObjectHashMap<ChunkPos>)map;
    }
    TFloatObjectHashMap<ChunkPos> floatChunkPosMap() {
        if (map==null) {map = (Object)new TFloatObjectHashMap(initCapacity, 0.8f, Float.MIN_VALUE);}
        return (TFloatObjectHashMap<ChunkPos>)map;
    }
    TDoubleObjectHashMap<ChunkPos> doubleChunkPosMap() {
        if (map==null) {map = (Object)new TDoubleObjectHashMap(initCapacity, 0.8f, Double.MIN_VALUE);}
        return (TDoubleObjectHashMap<ChunkPos>)map;
    }
    HashMap<Object, ChunkPos> objChunkPosMap() {
        if (map==null) {map = (Object)new HashMap(initCapacity, 0.8f);}
        return (HashMap<Object, ChunkPos>)map;
    }
	
	// add/remove/get/contains methods for int as value type
	public void addChunkPos(int i, ChunkPos chunkPos) { intChunkPosMap().put(i, chunkPos); }
	public void addChunkPos(long l, ChunkPos chunkPos) { longChunkPosMap().put(l, chunkPos); }
	public void addChunkPos(float f, ChunkPos chunkPos) { floatChunkPosMap().put(f, chunkPos); }
	public void addChunkPos(double d, ChunkPos chunkPos) { doubleChunkPosMap().put(d, chunkPos); }
	public void addChunkPos(Object o, ChunkPos chunkPos) { objChunkPosMap().put(o, chunkPos); }

	public void removeChunkPos(int i) { intChunkPosMap().remove(i);}
	public void removeChunkPos(long l) { longChunkPosMap().remove(l);}
	public void removeChunkPos(float f) { floatChunkPosMap().remove(f);}
	public void removeChunkPos(double d) { doubleChunkPosMap().remove(d); }
	public void removeChunkPos(Object o) { objChunkPosMap().remove(o); }
	
	public ChunkPos getChunkPos(int i) { return intChunkPosMap().get(i); }
	public ChunkPos getChunkPos(long l) { return longChunkPosMap().get(l);	}
	public ChunkPos getChunkPos(float f) { return floatChunkPosMap().get(f); }
	public ChunkPos getChunkPos(double d) { return doubleChunkPosMap().get(d); }
	public ChunkPos getChunkPos(Object o) { return objChunkPosMap().get(o); }

	// add/remove/get/contains methods for ChunkPos as value type
    public void add1(int i, int pos) { intIntMap().put(i, pos); }
    public void add1(long l, int pos) {
        longIntMap().put(l, pos);
    }
    public void add1(float f, int pos) {
        floatIntMap().put(f, pos);
    }
    public void add1(double d, int pos) {
        doubleIntMap().put(d, pos);
    }
    public void add1(Object o, int pos) { objIntMap().put(o, pos); }
    public void add1(Tuple t, int pos) { tupIntMap().put(t, pos); }

    public void remove1(int i) { intIntMap().remove(i);}
    public void remove1(long l) { longIntMap().remove(l);}
    public void remove1(float f) { floatIntMap().remove(f);}
    public void remove1(double d) { doubleIntMap().remove(d); }
    public void remove1(Object o) { objIntMap().remove(o); }
    public void remove1(Tuple t) { tupIntMap().remove(t); }

    public int get1(int i) { return intIntMap().get(i); }
    public int get1(long l) { return longIntMap().get(l);	}
    public int get1(float f) { return floatIntMap().get(f); }
    public int get1(double d) { return doubleIntMap().get(d); }
    public int get1(Object o) { return objIntMap().get(o); }
    public int get1(Tuple t) { return tupIntMap().get(t); }

    public boolean containsKey(int i) { return intIntMap().containsKey(i); }
    public boolean containsKey(long l) { return longIntMap().containsKey(l);	}
    public boolean containsKey(float f) { return floatIntMap().containsKey(f); }
    public boolean containsKey(double d) { return doubleIntMap().containsKey(d); }
    public boolean containsKey(Object o) { return objIntMap().containsKey(o); }
    public boolean containsKey(Tuple t) { return tupIntMap().containsKey(t); }


	public void clear() {
        map=null;
	}
}