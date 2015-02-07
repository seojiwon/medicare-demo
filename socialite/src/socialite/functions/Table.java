package socialite.functions;

import socialite.resource.DistTableSliceMap;
import socialite.resource.SRuntime;
import socialite.resource.SRuntimeWorker;
import socialite.resource.TableSliceMap;

import java.util.Map;

public class Table {
    public static int getId(String name) {
        SRuntime runtime = SRuntimeWorker.getInst();
        if (runtime==null) runtime = SRuntime.getInst();
        if (runtime==null) {
            System.err.println("runtime is null");
        }
        Map<String, socialite.parser.Table> map = runtime.getTableMap();
        if (map==null) {
            System.err.println("map is null");
        }
        if (map.get(name)==null) {
            System.err.println("map.get("+name+") is null");
        }
        return map.get(name).id();
    }

    public static int isLocal(int tableid, int partitionKey) {
        SRuntimeWorker runtime = SRuntimeWorker.getInst();
        if (runtime==null) return 1;

        DistTableSliceMap map = runtime.getSliceMap();
        return map.isLocal(tableid, partitionKey)?1:0;
    }
    public static int isLocal(int tableid, Object partitionKey) {
        SRuntimeWorker runtime = SRuntimeWorker.getInst();
        if (runtime==null) return 1;

        DistTableSliceMap map = runtime.getSliceMap();
        return map.isLocal(tableid, partitionKey)?1:0;
    }
}
