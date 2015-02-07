package socialite.tables;


import socialite.util.concurrent.ConcurrentIntSkipListMap;
import socialite.util.concurrent.MapEntry;
import socialite.visitors.IVisitor;
import socialite.visitors.VisitorImpl;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConDynSkipListTable {
    static final int CHUNK_SIZE=32;

    static final class Node {
        volatile Object col0;
        volatile int len;
        volatile Node next;
        ConDynSkipListTable table;

        Node(ConDynSkipListTable table) { this(CHUNK_SIZE, table); }
        Node(int capacity, ConDynSkipListTable table) {
            col0 = new int[capacity];
            len = 0;
            next = null;
            this.table = table;
        }

        public void print() {
            System.out.print("Node:");
            if (len<0) {
                System.out.print("[len:"+len+"]");
            }
            for (int i=0; i<len(); i++) {
                System.out.print(", "+col0()[i]);
            }
            System.out.print("  ");
        }
        public String toString() {
            String str="Node[";
            for (int i=0; i<len(); i++) {
                str += ", "+col0()[i];
            }
            str += "]";
            return str;
        }

        static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater =
                        AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");
        static final AtomicIntegerFieldUpdater<Node> lenUpdater =
                        AtomicIntegerFieldUpdater.newUpdater(Node.class, "len");
        boolean casNext(Node cmp, Node val) {
            return nextUpdater.compareAndSet(this, cmp, val);
        }
        boolean casLen(int cmp, int val) { return lenUpdater.compareAndSet(this, cmp, val); }

        int len() {
            int l=len;
            return l>=0?l:-l;
        }
        boolean isHead() { return col0().length==0; }
        boolean mark() {
            int l=len;
            if (l<0) return false;
            return casLen(l, -l);
        }
        boolean isMarked() { return len<0; }


        int[] col0() { return (int[])col0; }

        int first() { return col0()[0];}
        int last() { return col0()[len()-1];}

        boolean contains(int a0) {
            int pos = Arrays.binarySearch(col0(), 0, len(), a0);
            return pos>=0;
        }

        int binarySearch(int[] col, int l, int key) {
            int pos = Arrays.binarySearch(col, 0, l, key);
            if (pos<0) {
                return -(pos+1);
            }
            assert col[pos] == key;
            while (pos<l-1) {
                if (col[pos+1] == key) {
                    pos++;
                } else {
                    return pos+1;
                }
            }
            return l;
        }
        int insert(int a0) {
            int l = len();
            //<if sorted>
            int pos = binarySearch(col0(), l, a0);
            int[] col0=col0();
            if (l+1<=col0.length) { // inserted in the current node
                System.arraycopy(col0, pos, col0, pos + 1, l - pos);
                col0[pos] = a0;
                len = l+1;
            } else { // requires a new node
                Node n=new Node(table);
                int[] ncol0=n.col0();
                int l1=l/2, l2=l-l1;
                if (next==null && pos==l) {
                    ncol0[0] = a0;
                    n.len = 1;
                } else if (pos<l1) { // stored in the current node
                    len = l1+1;
                    n.len = l2;
                    System.arraycopy(col0, l1, ncol0, 0, l2);
                    System.arraycopy(col0, pos, col0, pos+1, l1-pos);
                    col0[pos] = a0;
                } else { // stored in the new node
                    len = l1;
                    n.len = l2+1;
                    int newpos = pos-l1;
                    System.arraycopy(col0, l1, ncol0, 0, newpos);
                    ncol0[newpos] = a0;
                    System.arraycopy(col0, pos, ncol0, newpos + 1, l2 - newpos);
                }
                n.next = this.next;
                this.next = n;
            }
            return pos;
        }
        int LOCKED_insert(int a0, Lock lock) {
            synchronized(this) {
                lock.unlock();
                return insert(a0);
            }
        }
        int split() {
            int l = len();
            int[] col0=col0();
            int x = col0[l/2];
            if (col0[0] == x && col0[l-1] == x)
                return l/2;

            for (int i=l/2-1; i>=0; i--) {
                if (col0[i]!=x)
                    return i+1;
            }
            for (int i=l/2; i<l; i++) {
                if (col0[i]!=x)
                    return i;
            }
            assert false:"impossible";
            return l/2;
        }
        boolean ATOMIC_insert(int a0, Lock lock, Node b) {
            synchronized(this) {
                lock.unlock();
                int l = len();
                if (isMarked()) return false;
                //<if sorted>
                int pos = binarySearch(col0(), l, a0);
                // XXX if contained, return false
                int[] col0=col0();
                if (l+1<=col0.length ) {
                    if (pos==l) { // in-place insertion in the current node
                        col0[pos] = a0;
                        len = l+1;
                        return true;
                    } else { // copied to a new node
                        if (!mark()) return false;

                        Node n=new Node(table);
                        int[] ncol0=n.col0();
                        n.len = l+1;
                        System.arraycopy(col0, 0, ncol0, 0, pos);
                        ncol0[pos] = a0;
                        System.arraycopy(col0, pos, ncol0, pos+1, l-pos);
                        n.next = this.next;
                        if (b.casNext(this, n) && !b.isMarked()) {
                            if (pos==0) { table.index0.remove(col0[0], this); }
                            table.index0.put(ncol0[0], n);
                            return true;
                        } else { return false; }
                    }
                } else { // requires 2 new nodes
                    if (!mark()) return false;

                    Node n1=new Node(table);
                    int[] n1col0=n1.col0();
                    Node n2=new Node(table);
                    int[] n2col0=n2.col0();

                    int l1=l/2, l2=l-l1;
                    if (col0[l1-1]==col0[l1]) {
                        l1 = split();
                        l2 = l-l1;
                    }
                    if (pos<l1 || col0[l1-1]==a0 && col0[l1]!=a0) { // a0 stored in n1
                        n1.len = l1+1;
                        n2.len = l2;

                        System.arraycopy(col0, 0, n1col0, 0, pos);
                        n1col0[pos] = a0;
                        System.arraycopy(col0, pos, n1col0, pos+1, l1-pos);

                        System.arraycopy(col0, l1, n2col0, 0, l2);

                        n1.next = n2;
                        n2.next = this.next;
                        if (b.casNext(this, n1) && !b.isMarked()) {
                            if (pos==0) { table.index0.remove(col0[0], this); }
                            table.index0.put(n1col0[0], n1);
                            table.index0.put(n2col0[0], n2);
                            return true;
                        } else { return false; }
                    } else { // a0 stored in n2
                        n1.len = l1;
                        n2.len = l2+1;
                        int newpos = pos-l1;

                        System.arraycopy(col0, 0, n1col0, 0, l1);
                        System.arraycopy(col0, l1, n2col0, 0, newpos);
                        n2col0[newpos] = a0;
                        System.arraycopy(col0, pos, n2col0, newpos + 1, l2 - newpos);
                        n1.next = n2;
                        n2.next = this.next;
                        if (b.casNext(this, n1) && !b.isMarked()) {
                            table.index0.put(col0[0], n1);
                            table.index0.put(n2col0[0], n2);
                            return true;
                        } else { return false; }
                    }
                }
            }
        }
        boolean iterate(IVisitor v) {
            for (int i=0; i<len(); i++) {
                boolean cont = v.visit(col0()[i]);
                if (!cont) return false;
            }
            return true;
        }
    }

    volatile Node head;
    ConcurrentIntSkipListMap<Node> index0;
    ReentrantLock lock = new ReentrantLock();

    ConDynSkipListTable() {
        head = new Node(0, this);
        index0 = new ConcurrentIntSkipListMap<Node>();
    }

    Node findPredecessor(int key) {
        MapEntry.IntKeyEntry<Node> entry = index0.lowerEntry(key);
        if (entry == null) return head;

        return entry.getValue();
    }
    Node findLastPredecessor(int key) {
        MapEntry.IntKeyEntry<Node> entry = index0.lowerEntry(key);
        if (entry==null) return head;

        Node n = entry.getValue();
        // XXX: find last predecessor...
        /*while (true) {
            ...
        }*/
        return n;
    }

    /*Node findLastPredecessorFrom(Node b, int key) {
        assert b.next!=null;
        assert b.first()==key && b.next.first()==key;
        while(true) {
            if (b.next.next==null)
                return b;

            if (b.next.next.first()==key)
                b = b.next;
            else return b;
        }
    }*/
    public boolean contains(int a0) {
        Node n = findPredecessor(a0);
        if (n.len() > 0 && n.last() < a0)
            n = n.next;
        while(true) {
            if (n.contains(a0))
                return true;
            n = n.next;
            if (n==null || n.first()!=a0)
                break;
        }
        return false;
    }

    public boolean insert(int a0) {
        Node q = head;
        while (true) {
            if (q.col0()==null) {
                if (q.next==null) {
                    q.next = new Node(this);
                    q = q.next;
                    break;
                } else if (q.next.col0()[0] > a0) {
                    Node f = q.next;
                    q.next = new Node(this);
                    q.next.next = f;
                    q = q.next;
                    break;
                }
            } else {
                if (q.next==null) {
                    break;
                } else if (q.next.col0()[0] > a0) {
                    break;
                }
            }
            q = q.next;
        }
        q.insert(a0);
        return true;
    }
    public boolean LOCKED_insert(int a0) {
        Node q = head;
        Lock l = lock;
        l.lock();
        while (true) {
            if (q.next == null) {
                q.next = new Node(this);
                q = q.next;
                break;
            } else if (q.next.col0()[0] > a0) {
                if (q==head) {
                    Node n=new Node(this);
                    n.next = q.next;
                    q.next = n;
                    q = n;
                }
                break;
            }
            q = q.next;
        }
        q.LOCKED_insert(a0, l);
        return true;
    }

    Node findInsNodePredecessor(int key) {
        // returns null if already contained.

        for (;;) {
            Node b = index0.findGrandPredecessor(key);
            Node n;
            Node f;

            if (b == null) {
                b = head;
                n = b.next;
                f = (n == null)?null:n.next;
            } else {
                n = b.next;
                f = (n == null)?null:n.next;
            }
            if (n == null) return b;
            if (n.next != f) // inconsistent read
                continue;
            if (f == null) return b;

            if (f.first() == key) {
                while (true) {
                    b = n;
                    n = f;
                    f = f.next;
                    if (b.next != n || n.next != f) // inconsistent read
                        continue;

                    if (f == null) {
                        return b;
                    }
                    if (f.first() != key) {
                        return b;
                    }
                }
            } else {
                return b;
            }
        }
    }
    public boolean ATOMIC_insert(int a0) {
        boolean success;
        int key = a0;
        Lock l = lock;
        l.lock();
        do {
            Node b, q;
            b = findInsNodePredecessor(key);
            // XXX if contains return false

            if (b.next==null) {
                q = new Node(this);
                if (!b.casNext(null, q)) {
                    success=false;
                    // release newly allocated node q
                    continue;
                }
            } else {
                q = b.next;
            }

            if (b.isMarked() || q.isMarked()) {
                success = false;
                continue;
            }
            success = q.ATOMIC_insert(a0, l, b);
        } while (!success);
        return true;
    }
    public boolean NoMap_ATOMIC_insert(int a0) {
        boolean success;
        do {
            Node b = head;
            Node q = head;
            Lock l = lock;
            l.lock();
            while (true) {
                if (q.next == null) {
                    q.next = new Node(this);
                    b = q;
                    q = q.next;
                    break;
                } else if (q.next.col0()[0] > a0) {
                    if (q == head) {
                        Node n = new Node(this);
                        n.next = q.next;
                        q.next = n;
                        q = n;
                    }
                    break;
                }
                b = q;
                q = q.next;
            }
            if (b.isMarked() || q.isMarked()) {
                success = false;
                continue;
            }
            success = q.ATOMIC_insert(a0, l, b);
        } while (!success);
        return true;
    }
    public void print() {
        System.out.println("Printing table...");
        Node n=head;
        int count=0;
        while (true) {
            n=n.next;
            if (n==null) break;
            n.print();
            count++;
            if (count%1==0) System.out.println();
        }
        System.out.println("");
    }
    /*public boolean ATOMIC_insert(int a0, double a1) {
        int idx=-1;
        while (true) {
            idx = tail.ATOMIC_append(a0, a1);
            if (idx>=0) { break; }

            Node n = new Node(this);
            Node t = tail;
            if (t.casNext(null, n)) {
                boolean success=casTail(t, n);
                assert success;
            }
        }
        assert idx>=0;
        groupby.addChunkPos(a0, new ChunkPos(tail, idx));
        return true;
    }*/

    public void iterate(IVisitor v) {
        Node h = head;
        for (Node n=h.next; n!=null; n=n.next) {
            n.iterate(v);
        }
    }

    public static void main(String[] args) {
        ConDynSkipListTable t = new ConDynSkipListTable();
        int cnt=0;
        for (int i=0; i<20; i+=2) {
            if (i==10) continue;
            t.ATOMIC_insert(i);
            cnt++;
        }
        for (int i=1; i<20; i+=2) {
            t.ATOMIC_insert(i);
            cnt++;
        }
        for (int i=20; i>=0; i-=2) {
            t.ATOMIC_insert(i);
            cnt++;
        }
        t.ATOMIC_insert(9);
        t.ATOMIC_insert(5);
        t.ATOMIC_insert(5);
        t.ATOMIC_insert(20);
        t.print();

        t.ATOMIC_insert(11);
        t.print();
        cnt+=5;

        Random r=new Random();
        for (int i=0; i<1000; i++) {
            int x = r.nextInt(500);
            t.ATOMIC_insert(x);
            cnt++;
        }
        t.print();



        /*t.ATOMIC_insert(11);
        t._print();
        t.ATOMIC_insert(11);
        t._print();*/

        /*for (int i=0; i<20; i+=2) {
            System.out.println("contains("+i+"):"+t.contains(i));
        }
        for (int i=1; i<20; i+=2) {
            System.out.println("contains("+i+"):"+t.contains(i));
        }
        for (int i=20; i>=0; i-=2) {
            System.out.println("contains("+i+"):"+t.contains(i));
        }*/
    }
    /*
    public static void atomic_insert_test() {
        ConDynSkipListTable t = new ConDynSkipListTable();
        int cnt=0;
        for (int i=0; i<20; i+=2) {
            t.ATOMIC_insert(i);
            cnt++;
        }
        for (int i=1; i<20; i+=2) {
            t.ATOMIC_insert(i);
            cnt++;
        }
        for (int i=20; i>=0; i-=2) {
            t.ATOMIC_insert(i);
            cnt++;
        }
        for (int i=40; i>=0; i-=2) {
            t.ATOMIC_insert(i);
            cnt++;
        }
        Random r=new Random();
        for (int i=0; i<100; i++) {
            int x = r.nextInt(100);
            t.ATOMIC_insert(x);
            cnt++;
        }
        System.out.println("Definitive count:"+cnt);

        t.iterate(new VisitorImpl() {
            int c=0;
            @Override
            public int getEpochId() {
                return 0;
            }

            @Override
            public int getRuleId() {
                return 0;
            }
            @Override
            public boolean visit(int a0) {
                c++;
                System.out.println(a0+", cnt:"+c);
                return true;
            }
        });
    }*/
}
