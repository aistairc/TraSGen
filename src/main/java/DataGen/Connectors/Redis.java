package DataGen.Connectors;

import org.jetbrains.annotations.NotNull;
import org.redisson.Redisson;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.IntegerCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Redis {

    private RedissonClient redissonClient;
    private RMap<String, Integer> roadSegmentsTrafficMap;

    public Redis() {
    }

    public RedissonClient createRedissonClient(@NotNull  String redisAddresses, @NotNull String serverType) throws Exception {

        Config config = new Config();
        config.setCodec(StringCodec.INSTANCE);

        if (serverType.equalsIgnoreCase("standalone"))
            config.useSingleServer().setAddress(redisAddresses);
        else if (serverType.equalsIgnoreCase("cluster")) {
            List<String> addresses = new ArrayList<String>(Arrays.asList(redisAddresses.split(", ")));
            for (String node: addresses) {
               config.useClusterServers().addNodeAddress(node);
            }

        }



        redissonClient = Redisson.create(config);

        return redissonClient;
    }

    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    public void shutdownRedissonClient() {
        if (redissonClient != null && !redissonClient.isShutdown()) {
            redissonClient.shutdown();
        }
    }

    public void createRoadSegmentsTrafficMap() {

        // Deletes a map if already exist and then creates a map
        if (!redissonClient.isShutdown()) {
            final RLock lock = redissonClient.getLock("lock");
            lock.lock();
            redissonClient.getMap("roadSegmentsTrafficMap", IntegerCodec.INSTANCE).delete();
            roadSegmentsTrafficMap = redissonClient.getMap("roadSegmentsTrafficMap", IntegerCodec.INSTANCE);
            lock.unlock();
        }
    }

    public void updateRoadSegmentsTrafficMap(String key, Integer value) {

        // Individual Map Key Lock
        RLock keyLock = roadSegmentsTrafficMap.getLock(key);
        try {
            keyLock.lock();
            // if this is the first value for this key, add it, else sum it with previous value
            roadSegmentsTrafficMap.merge(key, value, Integer::sum);
//            if(roadSegmentsTrafficMap.get(key) != null) {
//                roadSegmentsTrafficMap.put(key, (roadSegmentsTrafficMap.get(key) + value));
//            }else{
//                roadSegmentsTrafficMap.put(key, value);
//            }

        } finally {
            keyLock.unlock();
        }
    }

    public Integer updateRoadSegmentsTrafficMapAsync(String key, Integer valToIncrement) throws ExecutionException, InterruptedException {

        Integer finalVal;
        // Individual Map Key Lock
        RLock keyLock = roadSegmentsTrafficMap.getLock(key);
        Integer tempVal;

        try {
            RFuture<Integer> val;
            keyLock.lockAsync();
            val = roadSegmentsTrafficMap.getAsync(key);
            tempVal = val.get();
            if (tempVal == null) { tempVal = 0;}
            finalVal = tempVal + valToIncrement;
            roadSegmentsTrafficMap.putAsync(key, finalVal);

        } finally {
            keyLock.unlockAsync();
        }

        return finalVal;
    }

    public Integer getRoadSegmentsTrafficMapValue(String key) {

        Integer val;
        // Individual Map Key Lock
        RLock keyLock = roadSegmentsTrafficMap.getLock(key);

        try {
            keyLock.lock();
            val = roadSegmentsTrafficMap.get(key);

        } finally {
            keyLock.unlock();
        }

        if (val == null) { val = 0;}

        return val;
    }

    public Integer getRoadSegmentsTrafficMapValueAsync(String key) throws ExecutionException, InterruptedException{

        Integer finalVal;
        // Individual Map Key Lock
        RLock keyLock = roadSegmentsTrafficMap.getLock(key);

        try {
            RFuture<Integer> val;
            keyLock.lockAsync();
            val = roadSegmentsTrafficMap.getAsync(key);
            finalVal = val.get();
            if (finalVal == null) { finalVal = 0;}

        } finally {
            keyLock.unlockAsync();
        }

        return finalVal;
    }

    public Boolean deleteRoadSegmentObjCount(String key) {

        // Individual Map Key Lock
        RLock keyLock = roadSegmentsTrafficMap.getLock(key);

        try {
            keyLock.lock();
            roadSegmentsTrafficMap.remove(key);
        } finally {
            keyLock.unlock();
        }

        return true;
    }

    public void addRoadSegmentObjCount(String key, Integer val) {

        RLock keyLock = roadSegmentsTrafficMap.getLock(key);

        try {
            keyLock.lock();
            roadSegmentsTrafficMap.put(key, val);
        } finally {
            keyLock.unlock();
        }

    }
}
