/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link Executor} to be used when executing a job on an already running cluster.
 */
@Internal
public class YarnSessionClusterExecutor implements Executor {

	public static final String NAME = "yarn-session-cluster";

	private final YarnClusterClientFactory clusterClientFactory;

	public YarnSessionClusterExecutor() {
		this.clusterClientFactory = new YarnClusterClientFactory();
	}

	@Override
	public CompletableFuture<JobClient> execute(final Pipeline pipeline, final Configuration configuration) throws Exception {
		final JobGraph jobGraph = getJobGraph(pipeline, configuration);

		try (final YarnClusterDescriptor clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
			final ApplicationId clusterID = clusterClientFactory.getClusterId(configuration);
			checkState(clusterID != null);

			// TODO: 17.11.19 we cannot close the client here because we simply have a future of the client
			final ClusterClient<ApplicationId> clusterClient = clusterDescriptor.retrieve(clusterID);
			return ClientUtils.submitJobAndGetJobClient(clusterClient, jobGraph);
		}
	}

	private JobGraph getJobGraph(
			final Pipeline pipeline,
			final Configuration configuration) {

		checkNotNull(pipeline);
		checkNotNull(configuration);

		final ExecutionConfigAccessor executionConfigAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
		final JobGraph jobGraph = FlinkPipelineTranslationUtil
				.getJobGraph(pipeline, configuration, executionConfigAccessor.getParallelism());

		jobGraph.addJars(executionConfigAccessor.getJars());
		jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
		jobGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());

		return jobGraph;
	}
}
