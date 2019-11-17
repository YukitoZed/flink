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

package org.apache.flink.client.program;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.ExecutorServiceLoader;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Execution Environment for remote execution with the Client.
 */
public class ContextEnvironment extends ExecutionEnvironment {

	private final ExecutorServiceLoader executorServiceLoader;

	private final Configuration configuration;

	private final ClassLoader userClassloader;

	private final AtomicReference<JobExecutionResult> jobExecutionResult;

	private boolean alreadyCalled;

	ContextEnvironment(
			final ExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userCodeClassLoader,
			final AtomicReference<JobExecutionResult> jobExecutionResult) {
		super(executorServiceLoader, configuration);

		this.executorServiceLoader = checkNotNull(executorServiceLoader);
		this.configuration = checkNotNull(configuration);
		this.userClassloader = checkNotNull(userCodeClassLoader);
		this.jobExecutionResult = checkNotNull(jobExecutionResult);
		this.alreadyCalled = false;

		final int parallelism = configuration.get(CoreOptions.DEFAULT_PARALLELISM);
		if (parallelism > 0) {
			setParallelism(parallelism);
		}
		super.setUserClassloader(userCodeClassLoader);
	}

	public ExecutorServiceLoader getExecutorServiceLoader() {
		return executorServiceLoader;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		verifyExecuteIsCalledOnceWhenInDetachedMode();
		lastJobExecutionResult = super.execute(jobName);
		setJobExecutionResult(lastJobExecutionResult);
		return lastJobExecutionResult;
	}

	private void verifyExecuteIsCalledOnceWhenInDetachedMode() {
		if (alreadyCalled && !configuration.getBoolean(DeploymentOptions.ATTACHED)) {
			throw new InvalidProgramException(DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.EXECUTE_TWICE_MESSAGE);
		}
		alreadyCalled = true;
	}

	public void setJobExecutionResult(JobExecutionResult jobExecutionResult) {
		this.jobExecutionResult.set(jobExecutionResult);
	}

	@Override
	public String toString() {
		return "Context Environment (parallelism = " + (getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? "default" : getParallelism()) + ")";
	}

	public ClassLoader getUserClassloader() {
		return userClassloader;
	}

	// --------------------------------------------------------------------------------------------

	public static void setAsContext(ContextEnvironmentFactory factory) {
		initializeContextEnvironment(factory);
	}

	public static void unsetContext() {
		resetContextEnvironment();
	}
}
