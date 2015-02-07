package socialite.tables;

/**
 * Created by jseo on 10/7/14.
 */
public class ChunkPos {
    Object chunk;
    int pos;
    public ChunkPos() {}
    public ChunkPos(Object chunk, int pos) {
        this.chunk=chunk;
        this.pos=pos;
    }
    public void setChunk(Object _chunk) { chunk = _chunk; }
    public void setPos(int _pos) { pos = _pos; }

    public Object getChunk() { return chunk; }
    public int getPos() { return pos; }

    public String toString() {
        return "ChunkPos("+chunk+":"+pos+")";
    }
}
