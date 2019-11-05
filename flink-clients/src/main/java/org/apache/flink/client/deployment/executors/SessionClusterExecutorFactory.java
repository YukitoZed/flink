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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.ClusterMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.ExecutorFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link ExecutorFactory} for executing jobs on an existing (session) cluster.
 */
@Internal
public class SessionClusterExecutorFactory implements ExecutorFactory {

	private final ClusterClientServiceLoader clusterClientServiceLoader;

	public SessionClusterExecutorFactory() {
		this(new DefaultClusterClientServiceLoader());
	}

	public SessionClusterExecutorFactory(final ClusterClientServiceLoader clusterClientServiceLoader) {
		this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);
	}

	@Override
	public boolean isCompatibleWith(Configuration configuration) {
		return configuration.get(DeploymentOptions.CLUSTER_MODE).equals(ClusterMode.SESSION);
	}

	@Override
	public Executor getExecutor(Configuration configuration) {
		return new SessionClusterExecutor<>(clusterClientServiceLoader);
	}
}
