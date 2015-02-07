package socialite.collection;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import gnu.trove.iterator.TIntIterator;
import socialite.util.Assert;
import socialite.util.SociaLiteException;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.TIntList;
import gnu.trove.map.hash.TDoubleIntHashMap;
import gnu.trove.map.hash.TDoubleObjectHashMap;
import gnu.trove.map.hash.TFloatIntHashMap;
import gnu.trove.map.hash.TFloatObjectHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import socialite.util.concurrent.ConcurrentIntOrderedListMap;
import socialite.util.concurrent.ParUtil;
import socialite.visitors.IVisitor;
import socialite.visitors.IntVisitor;

public final class SIndex {
	Object index;

	public SIndex() {}
	public SIndex(int capacity, int columnNum) {
		this(columnNum);
	}
	public SIndex(int columnNum) { }

	public void clear() {
		if (index==null) return;
		
		if (index instanceof ConcurrentIntOrderedListMap) {
            iIndex().clear();
        } else {
			throw new SociaLiteException("Unexpected index class:"+index.getClass().getSimpleName());
		}
	}
	// int key
    ConcurrentIntOrderedListMap iIndex() {
		if (index==null) index = new ConcurrentIntOrderedListMap();
		return (ConcurrentIntOrderedListMap)index;
	}

	public void add(int key, int pos) {
        Object val = iIndex().get(key);
        if (val == null) {
            iIndex().put(key, pos);
        } else if (val instanceof Integer){
            int prev = (Integer)val;
            IntChunkedList l = new IntChunkedList();
            l.append(prev);
            l.append(pos);
            iIndex().put(key, l);
        } else {
            IntChunkedList l = (IntChunkedList)val;
            l.append(pos);
        }
	}
    public void addAtomic(int key, int pos) {
        for (;;) {
            Object val = iIndex().get(key);
            if (val == null) {
                Object old = iIndex().putAtomicIfAbsent(key, pos);
                if (old == null) break;
            } else if (val instanceof Integer){
                int prev = (Integer)val;
                IntChunkedList l = new IntChunkedList();
                l.append(prev);
                l.append(pos);
                boolean success = iIndex().replace(key, val, l);
                if (success) break;
            } else {
                IntChunkedList l = (IntChunkedList)val;
                l.appendAtomic(pos);
                break;
            }
        }
    }
    public boolean contains(int key) {
        return iIndex().contains(key);
    }
	public void iterateBy(int key, IntVisitor v) {
		Object val = iIndex().get(key);
        if (val==null) return;
        if (val instanceof Integer) {
            final int i = (Integer)val;
            v.visit(i);
        } else {
            assert val instanceof IntChunkedList;
            IntChunkedList l = (IntChunkedList)val;
            l.iterate(v);
        }
	}

    public static void main(String[] args) {
        final int N = 150000;
        final SIndex index = new SIndex();
        final int[] keys = new int[N];

        final int cutoff = 100013;
        final Random r = new Random();
        int i=0;
        while (i<N) {
            int x = r.nextInt()%cutoff;
            if (index.contains(x))
                continue;
            keys[i] = x;
            index.add(x, x);
            if (i%2==1)
                index.add(x, x+1);
            i++;
        }

        final int nThread = 16;
        final int nReader = 8;
        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger wcount = new AtomicInteger();
        long s = System.currentTimeMillis();
        for (int k=0; k<10; k++) {
            ParUtil.parallel(nThread, new ParUtil.CallableBlock() {
                public void call(int tIdx) {
                    if (tIdx < nReader) {
                        for (int j = 0; j < 70; j++) {
                            for (int i = tIdx; i < N; i += nReader) {
                                final int idx = i;
                                count.incrementAndGet();
                                index.iterateBy(keys[idx], new IntVisitor() {
                                    public boolean visit(int val) {
                                        if (idx % 2 == 0) {
                                            assert val == keys[idx] : "val:" + val + ", keys[idx]:" + keys[idx];
                                        } else {
                                            assert val == keys[idx] || val == keys[idx] + 1;
                                        }
                                        return true;
                                    }
                                });
                            }
                        }
                        //System.out.println("Reader #"+tIdx+" done");
                    } else {
                        for (int i = 0; i < N; i++) {
                            int x = r.nextInt()%cutoff;
                            index.addAtomic(x, x);
                            wcount.incrementAndGet();
                        }
                        //System.out.println("Writer #"+tIdx+" done");
                    }
                }
            });
            System.out.println("Finished all parallel... iteration:" + k);
        }
        long e = System.currentTimeMillis();
        System.out.println("Exec time:"+(e-s)+"ms, count:"+count.get()+", wcount:"+wcount.get());
    }
    static class IntChunkedList {
        static final int CHUNK_SIZE = 8;
        static final int INIT_CHUNK_SIZE = CHUNK_SIZE/2;
        Node head;
        Node tail;

        IntChunkedList() {
            head = new Node(INIT_CHUNK_SIZE);
            tail = head;
        }
        public void append(int i) {
            if (tail.append(i)) {
                return;
            } else {
                tail.next = new Node(CHUNK_SIZE);
                tail = tail.next;
                tail.append(i);
            }
        }
        public void appendAtomic(int i) {
            for (;;) {
                boolean success = tail.appendAtomic(i);
                if (success) return;

                Node n = new Node(CHUNK_SIZE);
                n.append(i);
                success = tail.casNext(null, n);
                if (success) {
                    tail = n;
                    return;
                }
            }
        }
        public void iterate(IntVisitor v) {
            for (Node n=head; n!=null; n=n.next) {
                int len = n.length;
                for (int i=0; i<len; i++) {
                    boolean cont = v.visit(n.vals[i]);
                    if (!cont) return;
                }
            }
        }

        static class Node {
            final int[] vals;
            volatile int length;
            volatile Node next;
            static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater =
                        AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
            static final AtomicIntegerFieldUpdater<Node> lenUpdater =
                        AtomicIntegerFieldUpdater.newUpdater(Node.class, "length");
            Node(int capacity) {
                vals = new int[capacity];
                length = 0;
            }
            boolean casNext(Node cmp, Node val) {
                return nextUpdater.compareAndSet(this, cmp, val);
            }
            void incLen() { lenUpdater.incrementAndGet(this); }

            public boolean append(int i) {
                if (length == vals.length)
                    return false;
                vals[length] = i;
                length++;
                return true;
            }
            public boolean appendAtomic(int i) {
                synchronized(this) {
                    if (length == vals.length)
                        return false;
                    vals[length] = i;
                    incLen();
                    return true;
                }
            }
        }
    }
}
