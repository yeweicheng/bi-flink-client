package org.yewc.flink.client.util;

import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;

public class FlinkClusterClient extends RestClusterClient {

    public FlinkClusterClient(Configuration config, Object clusterId) throws Exception {
        super(config, clusterId);
    }
}
