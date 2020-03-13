package org.yewc.flink.client.util;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.util.List;

public class FlinkClusterClient extends RestClusterClient {

    public FlinkClusterClient(Configuration config, Object clusterId) throws Exception {
        super(config, clusterId);
    }

    public FlinkClusterClient(Configuration config, Object clusterId, LeaderRetrievalService webMonitorRetrievalService) throws Exception {
        super(config, clusterId, webMonitorRetrievalService);
    }

    @Override
    public JobSubmissionResult run(FlinkPlan compiledPlan, List libraries, List classpaths, ClassLoader classLoader, SavepointRestoreSettings savepointSettings) throws ProgramInvocationException {
        libraries.addAll(classpaths);
        classpaths.clear();
        JobGraph job = getJobGraph(flinkConfig, compiledPlan, libraries, classpaths, savepointSettings);
        return super.submitJob(job, classLoader);
    }
}
