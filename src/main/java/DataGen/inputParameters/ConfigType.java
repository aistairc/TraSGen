/*
 * Copyright 2023 Data Platform Research Team, AIRC, AIST, Japan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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