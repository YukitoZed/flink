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
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.executors.JobClientImpl;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link Executor} to be used when executing a job in isolation.
 * This executor will start a cluster specifically for the job at hand and
 * tear it down when the job is finished either successfully or due to an error.
 */
@Internal
public class YarnJobClusterExecutor implements Executor {

	private static final Logger LOG = LoggerFactory.getLogger(YarnJobClusterExecutor.class);

	public static final String NAME = "yarn-job-cluster";

	private final YarnClusterClientFactory clusterClientFactory;

	public YarnJobClusterExecutor() {
		this.clusterClientFactory = new YarnClusterClientFactory();
	}

	@Override
	public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration executionConfig) throws Exception {

		try (final YarnClusterDescriptor clusterDescriptor = clusterClientFactory.createClusterDescriptor(executionConfig)) {
			final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(executionConfig);

			final List<URL> dependencies = configAccessor.getJars();
			final List<URL> classpaths = configAccessor.getClasspaths();

			final JobGraph jobGraph = getJobGraph(pipeline, executionConfig, classpaths, dependencies);

			final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(executionConfig);

			final ClusterClient<ApplicationId> client = clusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, configAccessor.getDetachedMode());
			LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());
			return CompletableFuture.completedFuture(new JobClientImpl<>(client, jobGraph.getJobID()));
		}
	}

	private JobGraph getJobGraph(
			final Pipeline pipeline,
			final Configuration configuration,
			final List<URL> classpaths,
			final List<URL> libraries) {

		checkNotNull(pipeline);
		checkNotNull(configuration);
		checkNotNull(classpaths);
		checkNotNull(libraries);

		final ExecutionConfigAccessor executionConfigAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
		final JobGraph jobGraph = FlinkPipelineTranslationUtil
				.getJobGraph(pipeline, configuration, executionConfigAccessor.getParallelism());

		jobGraph.addJars(libraries);
		jobGraph.setClasspaths(classpaths);
		jobGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());

		return jobGraph;
	}
}
