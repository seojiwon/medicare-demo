package socialite.tables;


import socialite.visitors.IVisitor;
import socialite.visitors.VisitorImpl;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class ConDynTable {
    static final int CHUNK_SIZE=4;

    static final class Node {
        volatile Object col0;
        volatile Object col1;
        volatile int wlen;
        volatile int len;
        volatile Node next;

        Node() { this(CHUNK_SIZE); }
        Node(int capacity) {
            col0 = new int[capacity];
            col1 = new double[capacity];
            len = 0;
            next=null;
        }

        Node(Node next) {
            col0=col1=null;
            len=-1;
            this.next = next;
        }
        static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater =
                        AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
        static final AtomicIntegerFieldUpdater<Node> wlenUpdater =
                        AtomicIntegerFieldUpdater.newUpdater(Node.class, "wlen");
        static final AtomicIntegerFieldUpdater<Node> lenUpdater =
                        AtomicIntegerFieldUpdater.newUpdater(Node.class, "len");
        boolean casNext(Node cmp, Node val) {
            return nextUpdater.compareAndSet(this, cmp, val);
        }
        boolean casWlen(int cmp, int val) {
            return wlenUpdater.compareAndSet(this, cmp, val);
        }
        int wlenIncAndGet() {
            return wlenUpdater.incrementAndGet(this);
        }
        boolean casLen(int cmp, int val) {
            return lenUpdater.compareAndSet(this, cmp, val);
        }

        int[] col0() { return (int[])col0; }
        double[] col1() { return (double[])col1; }

        int col0(int idx) { return ((int[])col0)[idx]; }
        double col1(int idx) { return ((double[])col1)[idx]; }

        boolean contains(int a0, double a1) {
            for (int i=0; i<len; i++) {
                if (col0(i)==a0 && col1(i)==a1) {
                    return true;
                }
            }
            return false;
        }
        boolean contains(int a0, double a1, boolean[] dontcare) {
            for (int i=0; i<len; i++) {
                if (dontcare[i] || (col0(i)==a0 && col1(i)==a1)) {
                    return true;
                }
            }
            return false;
        }
        int append(int a0, double a1) {
            int l=len++;
            wlen = l+1;
            col0()[l] = a0;
            col1()[l] = a1;
            return l;
        }

        int LOCKED_append(int a0, double a1) {
            synchronized(this) {
                return append(a0, a1);
            }
        }
        int ATOMIC_append(int a0, double a1) {
            if (wlen>=col0().length) return -1;

            int l = wlenIncAndGet();
            if (l>=col0().length) return -1;

            col0()[l] = a0;
            col1()[l] = a1;

            while (casLen(l-1, l)!=true) ;

            return l;
        }

        boolean isFull() {
            return len>=col0().length;
        }
        boolean iterate(IVisitor v) {
            for (int i=0; i<len; i++) {
                boolean cont = v.visit(col0(i), col1(i));
                if (!cont) return false;
            }
            return true;
        }
        boolean iterate_by_0(int a0, IVisitor v) {
            for (int i=0; i<len; i++) {
                if (col0(i)==a0) {
                    boolean cont=v.visit(col0(i), col1(i));
                    if (!cont) return false;
                }
            }
            return true;
        }
    }

    volatile Node head;
    volatile Node tail;
    GroupbyMap groupby;
    ConDynTable() {
        head = new Node(CHUNK_SIZE);
        tail = head;
        groupby = new GroupbyMap();
    }

    static final AtomicReferenceFieldUpdater<ConDynTable, Node> tailUpdater=
                        AtomicReferenceFieldUpdater.newUpdater(ConDynTable.class, Node.class, "tail");
    boolean casTail(Node cmp, Node val) {
        return tailUpdater.compareAndSet(this, cmp, val);
    }
    public boolean contains(int a0, double a1) {
        for (Node n=head; n!=null; n=n.next) {
            if (n.contains(a0, a1))
                return true;
        }
        return false;
    }
    public boolean contains(int a0, double a1, boolean[] dontcare) {
        for (Node n=head; n!=null; n=n.next) {
            if (n.contains(a0, a1, dontcare))
                return true;
        }
        return false;
    }
    public boolean insert(int a0, double a1) {
        if (tail.isFull()) {
            tail.next = new Node(CHUNK_SIZE);
            tail = tail.next;
        }
        int idx = tail.append(a0, a1);
        groupby.addChunkPos(a0, new ChunkPos(tail, idx));
        return true;
    }
    public boolean ATOMIC_insert(int a0, double a1) {
        int idx=-1;
        while (true) {
            idx = tail.ATOMIC_append(a0, a1);
            if (idx>=0) { break; }

            Node n = new Node();
            Node t = tail;
            if (t.casNext(null, n)) {
                boolean success=casTail(t, n);
                assert success;
            }
        }
        assert idx>=0;
        groupby.addChunkPos(a0, new ChunkPos(tail, idx));
        return true;
    }

    public double groupby(int a0) {
        ChunkPos chunkPos = groupby.getChunkPos(a0);
        Node n = (Node)chunkPos.chunk;
        int idx = chunkPos.pos;
        return n.col1(idx);
    }
    public boolean contains(int a0) {
        return groupby.getChunkPos(a0)!=null;
    }
    public boolean update(int a0, double a1) {
        ChunkPos chunkPos = groupby.getChunkPos(a0);
        Node n = (Node)chunkPos.chunk;
        int idx = chunkPos.pos;
        assert n.col0(idx)==a0;
        n.col1()[idx] = a1;
        return true;
    }
    public boolean LOCKED_update(int a0, double a1) {
        synchronized (this) {
            ChunkPos chunkPos = groupby.getChunkPos(a0);
            if (chunkPos!=null) return false;
            insert(a0, a1);
            return true;
        }
    }
    public boolean LOCKED_update(int a0, double old1, double a1) {
        synchronized (this) {
            ChunkPos chunkPos = groupby.getChunkPos(a0);
            if (chunkPos==null) return false;
            Node n = (Node)chunkPos.chunk;
            int idx = chunkPos.pos;
            if (n.col1(idx)!=old1) return false;

            n.col1()[idx] = a1;
            return true;
        }
    }

    public void iterate(IVisitor v) {
        Node h = head;
        for (Node n=head; n!=null; n=n.next) {
            n.iterate(v);
        }
    }

    public void iterate_by_0(int a0, IVisitor v) {
        Node h = head;
        for (Node n=head; n.next!=null; n=n.next) {
            n.iterate_by_0(a0, v);
        }
    }

    public static void main(String[] args) {
        ConDynTable t = new ConDynTable();

        for (int i=0; i<10; i++) {
            t.ATOMIC_insert(i, (i+1)/10.0);
        }
        double d=t.groupby(1);
        //t.ATOMIC_update(1, d, d+100.0);

        t.iterate(new VisitorImpl() {
            @Override
            public int getEpochId() {
                return 0;
            }

            @Override
            public int getRuleId() {
                return 0;
            }
            @Override
            public boolean visit(int a0, double a1) {
                System.out.println(a0+","+a1);
                return true;
            }
        });

    }
}