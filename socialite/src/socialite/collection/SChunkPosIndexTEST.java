package socialite.collection;

import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import socialite.tables.ChunkPos;
import socialite.util.Assert;
import socialite.util.concurrent.*;

import java.util.concurrent.ConcurrentSkipListMap;

import socialite.visitors.ObjectVisitor;

public final class SChunkPosIndexTEST {
	volatile Object index;

	public SChunkPosIndexTEST() {}

	public void clear() {
		if (index==null) return;

        if (index instanceof TIntObjectHashMap) iIndex().clear();
        else if (index instanceof ConcurrentLongOrderedListMap) lIndex().clear();
        else if (index instanceof ConcurrentFloatOrderedListMap) fIndex().clear();
        else if (index instanceof ConcurrentDoubleOrderedListMap) dIndex().clear();
        else if (index instanceof ConcurrentSkipListMap) oIndex().clear();
        else {
            Assert.impossible("Unexpected index type:"+index.getClass().getSimpleName());
        }
	}

    void visitVal(Object val, ObjectVisitor v) {
        if (val == null) return;
        if (val instanceof ChunkPos) {
            v.visit(val);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
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
		return (TIntObjectHashMap )index;
	}

	public void add(int key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = iIndex().get(key);
		if (val == null) {
            iIndex().put(key, pos);
		} else if (val instanceof ChunkPos) {
            MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
            posList.add((ChunkPos)val);
            posList.add(pos);
            iIndex().put(key, posList);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.add(pos);
        }
	}
    public Object get(int key) {
        return iIndex().get(key);
    }
	public void remove(int key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = iIndex().get(key);
        if (val == null) {
            return;
        } else if (val instanceof ChunkPos) {
            iIndex().remove(key);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.remove(pos);
        }
	}
    public void iterateBy(int key, ObjectVisitor v) {
        Object val = iIndex().get(key);
        visitVal(val, v);
    }
    void iterate(ConcurrentIntOrderedListMap.IntObjectIterator iter, ObjectVisitor v) {
        try {
            while (iter.hasNext()) {
                iter.next();
                Object val = iter.value();
                visitVal(val, v);
            }
        } finally { iter.cleanup(); }
    }
    

    // long-type key
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ConcurrentLongOrderedListMap lIndex() {
        if (index == null) {
            synchronized (this) {
                if (index == null) { index = new ConcurrentLongOrderedListMap(); }
            }
        }
        return (ConcurrentLongOrderedListMap )index;
    }

    public void addAtomic(long key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        for (;;) {
            Object val = lIndex().get(key);
            if (val == null) {
                boolean success = lIndex().putAtomicIfAbsent(key, pos) == null;
                if (success)
                    return;
            } else if (val instanceof ChunkPos) {
                MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
                posList.add((ChunkPos)val);
                posList.add(pos);
                boolean success = lIndex().replace(key, val, posList);
                if (success)
                    return;
            } else {
                MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>) val;
                posList.addAtomic(pos);
                return;
            }
        }
    }
    public void add(long key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = lIndex().get(key);
        if (val == null) {
            lIndex().put(key, pos);
        } else if (val instanceof ChunkPos) {
            MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
            posList.add((ChunkPos)val);
            posList.add(pos);
            lIndex().put(key, posList);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.add(pos);
        }
    }
    public Object get(long key) { return lIndex().get(key); }
    public void remove(long key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = lIndex().get(key);
        if (val == null) {
            return;
        } else if (val instanceof ChunkPos) {
            lIndex().remove(key);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.remove(pos);
        }
    }
    public void iterateBy(long key, ObjectVisitor v) {
        Object val = lIndex().get(key);
        visitVal(val, v);
    }
    void iterate(ConcurrentLongOrderedListMap.LongObjectIterator iter, ObjectVisitor v) {
        try {
            while (iter.hasNext()) {
                iter.next();
                Object val = iter.value();
                visitVal(val, v);
            }
        } finally { iter.cleanup(); }
    }
    public void iterateFrom(long from, boolean fromInclusive, ObjectVisitor v) {
        ConcurrentLongOrderedListMap.LongObjectIterator iter = lIndex().iteratorFrom(from, fromInclusive);
        iterate(iter, v);
    }
    public void iterateTo(long to, boolean toInclusive, ObjectVisitor v) {
        ConcurrentLongOrderedListMap.LongObjectIterator iter = lIndex().iteratorTo(to, toInclusive);
        iterate(iter, v);
    }
    public void iterateFromTo(long from, boolean fromInclusive, long to, boolean toInclusive, ObjectVisitor v) {
        ConcurrentLongOrderedListMap.LongObjectIterator iter = lIndex().iterator(from, to, fromInclusive, toInclusive);
        iterate(iter, v);
    }

    // float-type key
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ConcurrentFloatOrderedListMap fIndex() {
        if (index == null) {
            synchronized (this) {
                if (index == null) { index = new ConcurrentFloatOrderedListMap(); }
            }
        }
        return (ConcurrentFloatOrderedListMap)index;
    }

    public void addAtomic(float key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        for (;;) {
            Object val = fIndex().get(key);
            if (val == null) {
                boolean success = fIndex().putAtomicIfAbsent(key, pos) == null;
                if (success)
                    return;
            } else if (val instanceof ChunkPos) {
                MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
                posList.add((ChunkPos)val);
                posList.add(pos);
                boolean success = fIndex().replace(key, val, posList);
                if (success)
                    return;
            } else {
                MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>) val;
                posList.addAtomic(pos);
                return;
            }
        }
    }
    public void add(float key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = fIndex().get(key);
        if (val == null) {
            fIndex().put(key, pos);
        } else if (val instanceof ChunkPos) {
            MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
            posList.add((ChunkPos)val);
            posList.add(pos);
            fIndex().put(key, posList);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.add(pos);
        }
    }
    public Object get(float key) { return fIndex().get(key); }
    public void remove(float key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = fIndex().get(key);
        if (val == null) {
            return;
        } else if (val instanceof ChunkPos) {
            fIndex().remove(key);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.remove(pos);
        }
    }
    public void iterateBy(float key, ObjectVisitor v) {
        Object val = fIndex().get(key);
        visitVal(val, v);
    }
    void iterate(ConcurrentFloatOrderedListMap.FloatObjectIterator iter, ObjectVisitor v) {
        try {
            while (iter.hasNext()) {
                iter.next();
                Object val = iter.value();
                visitVal(val, v);
            }
        } finally { iter.cleanup(); }
    }
    public void iterateFrom(float from, boolean fromInclusive, ObjectVisitor v) {
        ConcurrentFloatOrderedListMap.FloatObjectIterator iter = fIndex().iteratorFrom(from, fromInclusive);
        iterate(iter, v);
    }
    public void iterateTo(float to, boolean toInclusive, ObjectVisitor v) {
        ConcurrentFloatOrderedListMap.FloatObjectIterator iter = fIndex().iteratorTo(to, toInclusive);
        iterate(iter, v);
    }
    public void iterateFromTo(float from, boolean fromInclusive, float to, boolean toInclusive, ObjectVisitor v) {
        ConcurrentFloatOrderedListMap.FloatObjectIterator iter = fIndex().iterator(from, to, fromInclusive, toInclusive);
        iterate(iter, v);
    }

	// double-type key
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ConcurrentDoubleOrderedListMap dIndex() {
        if (index == null) {
            synchronized (this) {
                if (index == null) { index = new ConcurrentDoubleOrderedListMap(); }
            }
        }
        return (ConcurrentDoubleOrderedListMap)index;
    }

    public void addAtomic(double key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        for (;;) {
            Object val = dIndex().get(key);
            if (val == null) {
                boolean success = dIndex().putAtomicIfAbsent(key, pos) == null;
                if (success)
                    return;
            } else if (val instanceof ChunkPos) {
                MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
                posList.add((ChunkPos)val);
                posList.add(pos);
                boolean success = dIndex().replace(key, val, posList);
                if (success)
                    return;
            } else {
                MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>) val;
                posList.addAtomic(pos);
                return;
            }
        }
    }
    public void add(double key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = dIndex().get(key);
        if (val == null) {
            dIndex().put(key, pos);
        } else if (val instanceof ChunkPos) {
            MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
            posList.add((ChunkPos)val);
            posList.add(pos);
            dIndex().put(key, posList);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.add(pos);
        }
    }
    public Object get(double key) { return dIndex().get(key); }
    public void remove(double key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = dIndex().get(key);
        if (val == null) {
            return;
        } else if (val instanceof ChunkPos) {
            dIndex().remove(key);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.remove(pos);
        }
    }
    public void iterateBy(double key, ObjectVisitor v) {
        Object val = dIndex().get(key);
        visitVal(val, v);
    }
    void iterate(ConcurrentDoubleOrderedListMap.DoubleObjectIterator iter, ObjectVisitor v) {
        try {
            while (iter.hasNext()) {
                iter.next();
                Object val = iter.value();
                visitVal(val, v);
            }
        } finally { iter.cleanup(); }
    }
    public void iterateFrom(double from, boolean fromInclusive, ObjectVisitor v) {
        ConcurrentDoubleOrderedListMap.DoubleObjectIterator iter = dIndex().iteratorFrom(from, fromInclusive);
        iterate(iter, v);
    }
    public void iterateTo(double to, boolean toInclusive, ObjectVisitor v) {
        ConcurrentDoubleOrderedListMap.DoubleObjectIterator iter = dIndex().iteratorTo(to, toInclusive);
        iterate(iter, v);
    }
    public void iterateFromTo(double from, boolean fromInclusive, double to, boolean toInclusive, ObjectVisitor v) {
        ConcurrentDoubleOrderedListMap.DoubleObjectIterator iter = dIndex().iterator(from, to, fromInclusive, toInclusive);
        iterate(iter, v);
    }

    // Object-type key
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ConcurrentSkipListMap oIndex() {
        if (index == null) {
            synchronized (this) {
                if (index == null) { index = new ConcurrentSkipListMap(); }
            }
        }
        return (ConcurrentSkipListMap)index;
    }

    public void addAtomic(Object key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        for (;;) {
            Object val = oIndex().get(key);
            if (val == null) {
                boolean success = oIndex().putIfAbsent(key, pos) == null;
                if (success)
                    return;
            } else if (val instanceof ChunkPos) {
                MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
                posList.add((ChunkPos)val);
                posList.add(pos);
                boolean success = oIndex().replace(key, val, posList);
                if (success)
                    return;
            } else {
                MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>) val;
                posList.addAtomic(pos);
                return;
            }
        }
    }
    public void add(Object key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = oIndex().get(key);
        if (val == null) {
            oIndex().put(key, pos);
        } else if (val instanceof ChunkPos) {
            MyIndexPosList<ChunkPos> posList = new MyIndexPosList<ChunkPos>();
            posList.add((ChunkPos)val);
            posList.add(pos);
            oIndex().put(key, posList);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.add(pos);
        }
    }
    public Object get(Object key) { return oIndex().get(key); }
    public void remove(Object key, ChunkPos pos) {
        if (pos==null)
            throw new NullPointerException();
        Object val = oIndex().get(key);
        if (val == null) {
            return;
        } else if (val instanceof ChunkPos) {
            oIndex().remove(key);
        } else {
            MyIndexPosList<ChunkPos> posList = (MyIndexPosList<ChunkPos>)val;
            posList.remove(pos);
        }
    }
    public void iterateBy(Object key, ObjectVisitor v) {
        Object val = oIndex().get(key);
        visitVal(val, v);
    }

    public void iterateFrom(Object from, boolean fromInclusive, ObjectVisitor v) {
        ConcurrentNavigableMap map = oIndex().tailMap(from, fromInclusive);
        for (Object key:map.keySet()) {
            Object val = map.get(key);
            visitVal(val, v);
        }
    }
    public void iterateTo(Object to, boolean toInclusive, ObjectVisitor v) {
        ConcurrentNavigableMap map = oIndex().headMap(to, toInclusive);
        for (Object key:map.keySet()) {
            Object val = map.get(key);
            visitVal(val, v);
        }
    }
    public void iterateFromTo(Object from, boolean fromInclusive, Object to, boolean toInclusive, ObjectVisitor v) {
        ConcurrentNavigableMap map = oIndex().subMap(from, fromInclusive, to, toInclusive);
        for (Object key:map.keySet()) {
            Object val = map.get(key);
            visitVal(val, v);
        }
    }

    static void testObjIndex() {
        SChunkPosIndexTEST index = new SChunkPosIndexTEST();
        for (int i=0; i<10; i++) {
            index.add("key"+i, new ChunkPos(null, i));
        }
        final int[] sum = new int[]{0};
        index.iterateFrom("key" + 2, true, new ObjectVisitor() {
            public boolean visit(Object o) {
                ChunkPos chunkPos = (ChunkPos) o;
                sum[0] += chunkPos.getPos();
                return true;
            }
        });
        assert sum[0] == (2+3+4+5+6+7+8+9);
        sum[0]=0;
        index.iterateTo("key" + 7, false, new ObjectVisitor() {
            public boolean visit(Object o) {
                ChunkPos chunkPos = (ChunkPos) o;
                sum[0] += chunkPos.getPos();
                return true;
            }
        });
        assert sum[0] == (0+1+2+3+4+5+6);
    }
    static void testFloatIndex() {
        SChunkPosIndexTEST index = new SChunkPosIndexTEST();
        for (int i=0; i<10; i++) {
            index.add((float)i, new ChunkPos(null, i));
        }
        final int[] sum = new int[]{0};
        class MyObjectVisitor_contains1 implements ObjectVisitor {
            public boolean visit(Object o) {
                ChunkPos chunkPos = (ChunkPos)o;
                sum[0] += chunkPos.getPos();
                return true;
            }
        }
        index.iterateFrom(2f, true, new MyObjectVisitor_contains1());
        assert sum[0] == (2+3+4+5+6+7+8+9);
        sum[0]=0;
        index.iterateTo(7f, false, new ObjectVisitor() {
            public boolean visit(Object o) {
                ChunkPos chunkPos = (ChunkPos)o;
                sum[0] += chunkPos.getPos();
                return true;
            }
        });
        assert sum[0] == (0+1+2+3+4+5+6);
    }
    public static void main(String[] args) {
        //testObjIndex();
        //testFloatIndex();
    	int sum=0;
    	long s= System.currentTimeMillis();
    	for (int j=0; j<100; j++) {
    	for (int i=0; i<256*1024; i++) {
    		sum+=i;
    	}
    	}
    	System.out.println("Exec time:"+(System.currentTimeMillis()-s)+"ms, sum=:"+sum);
    }
    
    static class MyIndexPosList<V> {
        static final int CHUNK_SIZE=8;
        static final class Node {
            Object[] elems;
            volatile int len;
            volatile Node next;

            public Node() {
                this(CHUNK_SIZE);
            }
            public Node(int capacity) {
                elems = new Object[capacity];
                len = 0;
                next = null;
            }
            public int len() {
                int l = len;
                if (l < 0) return -l;
                else return l;
            }
            public boolean isFull() {
                return len() >= elems.length;
            }

            public boolean contains(Object val) {
                int l = len();
                for (int i=0; i<l; i++) {
                    if (elems[i].equals(val))
                        return true;
                }
                return false;
            }
            public void add(Object val) {
                int l = len();
                elems[l] = val;
                len = l+1;
            }
            public boolean addAtomic(Object val) {
                if (isFull()) return false;
                synchronized (this) {
                    if (isFull())
                        return false;
                    int l = len();
                    elems[l] = val;
                    len = l+1;
                    return true;
                }
            }
            public boolean remove(Object val) {
                int l = len();
                for (int i=0; i<l; i++) {
                    if (val.equals(elems[i])) {
                        elems[i] = null;
                        return true;
                    }
                }
                return false;
            }
            public boolean iterate(ObjectVisitor v) {
                int l = len();
                boolean cont = false;
                for (int i = 0; i < l; i++) {
                    Object e = elems[i];
                    if (e != null)
                        cont = cont | v.visit(e);
                    if (!cont) break;
                }
                return cont;
            }
        }

        volatile Node head;
        volatile Node tail;
        public MyIndexPosList() {
            head = new Node();
            tail = head;
        }
        static final AtomicReferenceFieldUpdater<MyIndexPosList, Node> tailUpdater =
                AtomicReferenceFieldUpdater.newUpdater(MyIndexPosList.class, Node.class, "tail");
        boolean casTail(Node cmp, Node val) { return tailUpdater.compareAndSet(this, cmp, val); }

        public boolean contains(int v) {
            for (Node n=head; n!=null; n=n.next) {
                if (n.contains(v))
                    return true;
            }
            return false;
        }
        public void add(V val) {
            if (tail.isFull()) {
                tail.next = new Node();
                tail = tail.next;
            }
            tail.add(val);
        }
        public void addAtomic(V val) {
            for (;;) {
                Node x = tail;
                boolean success = x.addAtomic(val);
                if (success) {
                    break;
                } else {
                    Node n = new Node();
                    if (casTail(x, n)) {
                        x.next = n;
                    }
                }
            }
        }
        public void remove(V val) {
            for (Node n=head; n!=null; n=n.next) {
                if (n.remove(val)) {
                    return;
                }
            }
        }
        public void iterate(ObjectVisitor v) {
            for (Node n=head; n!=null; n=n.next) {
                boolean cont = n.iterate(v);
                if (!cont) return;
            }
        }
    }
}

