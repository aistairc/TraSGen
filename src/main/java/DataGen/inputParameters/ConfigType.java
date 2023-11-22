package DataGen.inputParameters;

import java.util.Map;

public class ConfigType {
    private boolean clusterMode;
    private int parallelism;
    private Map<String, Object> output;
    private Map<String, Object> query;
    private Map<String, Object> hotspotQuery;
    private Map<String, Object> mappedTrajectories;
    private Map<String, Object> redis;

    public boolean isClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(boolean clusterMode) {
        this.clusterMode = clusterMode;
    }
    public int getParallelism() { return parallelism;}

    public void setParallelism(int parallelism) { this.parallelism = parallelism;}

    public Map<String, Object> getOutput() {
        return output;
    }

    public void setOutput(Map<String, Object> output) {
        this.output = output;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public void setRedis(Map<String, Object> redis) {
        this.redis = redis;
    }

    public Map<String, Object> getRedis() {
        return redis;
    }

    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }

    public Map<String, Object> getHotspotQuery() {
        return hotspotQuery;
    }

    public void setHotspotQuery(Map<String, Object> hotspotQuery) {
        this.hotspotQuery = hotspotQuery;
    }

    public Map<String, Object> getMappedTrajectories() {
        return mappedTrajectories;
    }

    public void setMappedTrajectories(Map<String, Object> mappedTrajectories) {
        this.mappedTrajectories = mappedTrajectories;
    }

    @Override
    public String toString() {
        return "clusterMode=" + clusterMode + ", output=" + output + ", query=" + query +
                ", hotspotQuery=" + hotspotQuery + ", mappedTrajectories=" + mappedTrajectories;
    }


}