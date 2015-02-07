package socialite.tables;


import socialite.visitors.IVisitor;
import socialite.visitors.VisitorImpl;

public class DynTable {
    static final int CHUNK_SIZE=4;

    static final class Node {
        volatile Object col0;
        volatile Object col1;
        volatile int len;
        volatile Node next;

        Node(int capacity) {
            col0 = new int[capacity];
            col1 = new double[capacity];
            len = 0;
            next=null;
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
            col0()[l] = a0;
            col1()[l] = a1;
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

    Node head;
    Node tail;
    GroupbyMap groupby;
    DynTable() {
        head = new Node(CHUNK_SIZE);
        tail = head;
        groupby = new GroupbyMap();
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
        DynTable t = new DynTable();

        for (int i=0; i<10; i++) {
            t.insert(i, (i+1)/10.0);
        }
        double d=t.groupby(1);
        t.update(1, d+100.0);

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