/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.overlord.autoscaling;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 */

public class SimpleWorkerProvisioningStrategy extends AbstractWorkerProvisioningStrategy
{
  private static final EmittingLogger log = new EmittingLogger(SimpleWorkerProvisioningStrategy.class);

  private final SimpleWorkerProvisioningConfig config;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;

  @Inject
  public SimpleWorkerProvisioningStrategy(
      SimpleWorkerProvisioningConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningSchedulerConfig provisioningSchedulerConfig
  )
  {
    this(
        config,
        workerConfigRef,
        provisioningSchedulerConfig,
        new Supplier<>()
        {
          @Override
          public ScheduledExecutorService get()
          {
            return ScheduledExecutors.fixed(1, "SimpleResourceManagement-manager--%d");
          }
        }
    );
  }

  public SimpleWorkerProvisioningStrategy(
      SimpleWorkerProvisioningConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningSchedulerConfig provisioningSchedulerConfig,
      Supplier<ScheduledExecutorService> execFactory
  )
  {
    super(provisioningSchedulerConfig, execFactory);
    this.config = config;
    this.workerConfigRef = workerConfigRef;
  }

  @Override
  public Provisioner makeProvisioner(WorkerTaskRunner runner)
  {
    return new SimpleProvisioner(runner);
  }

  private class SimpleProvisioner implements Provisioner
  {
    private final WorkerTaskRunner runner;
    private final ScalingStats scalingStats = new ScalingStats(config.getNumEventsToTrack());

    private final Set<String> currentlyProvisioning = new HashSet<>();
    private final Set<String> currentlyTerminating = new HashSet<>();

    private int targetWorkerCount = -1;
    private DateTime lastProvisionTime = DateTimes.nowUtc();
    private DateTime lastTerminateTime = lastProvisionTime;

    SimpleProvisioner(WorkerTaskRunner runner)
    {
      this.runner = runner;
    }

    @Override
    public synchronized boolean doProvision()
    {
      Collection<? extends TaskRunnerWorkItem> pendingTasks = runner.getPendingTasks();
      Collection<ImmutableWorkerInfo> workers = runner.getWorkers();
      boolean didProvision = false;
      final DefaultWorkerBehaviorConfig workerConfig =
          PendingTaskBasedWorkerProvisioningStrategy.getDefaultWorkerBehaviorConfig(workerConfigRef, config, "provision", log);
      if (workerConfig == null) {
        return false;
      }

      final Predicate<ImmutableWorkerInfo> isValidWorker = ProvisioningUtil.createValidWorkerPredicate(config);
      final int currValidWorkers = Collections2.filter(workers, isValidWorker).size();

      final List<String> workerNodeIds = workerConfig.getAutoScaler().ipToIdLookup(
          Lists.newArrayList(
              Iterables.transform(
                  workers,
                  new Function<>()
                  {
                    @Override
                    public String apply(ImmutableWorkerInfo input)
                    {
                      return input.getWorker().getIp();
                    }
                  }
              )
          )
      );
      currentlyProvisioning.removeAll(workerNodeIds);

      updateTargetWorkerCount(workerConfig, pendingTasks, workers);

      int want = targetWorkerCount - (currValidWorkers + currentlyProvisioning.size());
      while (want > 0) {
        final AutoScalingData provisioned = workerConfig.getAutoScaler().provision();
        final List<String> newNodes;
        if (provisioned == null || (newNodes = provisioned.getNodeIds()).isEmpty()) {
          log.warn("NewNodes is empty, returning from provision loop");
          break;
        } else {
          currentlyProvisioning.addAll(newNodes);
          lastProvisionTime = DateTimes.nowUtc();
          scalingStats.addProvisionEvent(provisioned);
          want -= provisioned.getNodeIds().size();
          didProvision = true;
        }
      }

      if (!currentlyProvisioning.isEmpty()) {
        Duration durSinceLastProvision = new Duration(lastProvisionTime, DateTimes.nowUtc());
        log.info("%s provisioning. Current wait time: %s", currentlyProvisioning, durSinceLastProvision);
        if (durSinceLastProvision.isLongerThan(config.getMaxScalingDuration().toStandardDuration())) {
          log.makeAlert("Worker node provisioning taking too long!")
             .addData("millisSinceLastProvision", durSinceLastProvision.getMillis())
             .addData("provisioningCount", currentlyProvisioning.size())
             .emit();

          workerConfig.getAutoScaler().terminateWithIds(Lists.newArrayList(currentlyProvisioning));
          currentlyProvisioning.clear();
        }
      }

      return didProvision;
    }

    @Override
    public synchronized boolean doTerminate()
    {
      Collection<? extends TaskRunnerWorkItem> pendingTasks = runner.getPendingTasks();
      final DefaultWorkerBehaviorConfig workerConfig =
          PendingTaskBasedWorkerProvisioningStrategy.getDefaultWorkerBehaviorConfig(workerConfigRef, config, "terminate", log);
      if (workerConfig == null) {
        return false;
      }

      boolean didTerminate = false;
      final Set<String> workerNodeIds = Sets.newHashSet(
          workerConfig.getAutoScaler().ipToIdLookup(
              Lists.newArrayList(
                  Iterables.transform(
                      runner.getLazyWorkers(),
                      new Function<>()
                      {
                        @Override
                        public String apply(Worker input)
                        {
                          return input.getIp();
                        }
                      }
                  )
              )
          )
      );

      currentlyTerminating.retainAll(workerNodeIds);

      Collection<ImmutableWorkerInfo> workers = runner.getWorkers();
      updateTargetWorkerCount(workerConfig, pendingTasks, workers);

      if (currentlyTerminating.isEmpty()) {

        final int excessWorkers = (workers.size() + currentlyProvisioning.size()) - targetWorkerCount;
        if (excessWorkers > 0) {
          final Predicate<ImmutableWorkerInfo> isLazyWorker = ProvisioningUtil.createLazyWorkerPredicate(config);
          final Collection<String> laziestWorkerIps =
              Collections2.transform(
                  runner.markWorkersLazy(isLazyWorker, excessWorkers),
                  new Function<>()
                  {
                    @Override
                    public String apply(Worker worker)
                    {
                      return worker.getIp();
                    }
                  }
              );
          if (laziestWorkerIps.isEmpty()) {
            log.info("Wanted to terminate %,d workers, but couldn't find any lazy ones!", excessWorkers);
          } else {
            log.info(
                "Terminating %,d workers (wanted %,d): %s",
                laziestWorkerIps.size(),
                excessWorkers,
                Joiner.on(", ").join(laziestWorkerIps)
            );

            final AutoScalingData terminated = workerConfig.getAutoScaler()
                                                           .terminate(ImmutableList.copyOf(laziestWorkerIps));
            if (terminated != null) {
              currentlyTerminating.addAll(terminated.getNodeIds());
              lastTerminateTime = DateTimes.nowUtc();
              scalingStats.addTerminateEvent(terminated);
              didTerminate = true;
            }
          }
        }
      } else {
        Duration durSinceLastTerminate = new Duration(lastTerminateTime, DateTimes.nowUtc());

        log.info("%s terminating. Current wait time: %s", currentlyTerminating, durSinceLastTerminate);

        if (durSinceLastTerminate.isLongerThan(config.getMaxScalingDuration().toStandardDuration())) {
          log.makeAlert("Worker node termination taking too long!")
             .addData("millisSinceLastTerminate", durSinceLastTerminate.getMillis())
             .addData("terminatingCount", currentlyTerminating.size())
             .emit();

          currentlyTerminating.clear();
        }
      }

      return didTerminate;
    }


    private void updateTargetWorkerCount(
        final DefaultWorkerBehaviorConfig workerConfig,
        final Collection<? extends TaskRunnerWorkItem> pendingTasks,
        final Collection<ImmutableWorkerInfo> zkWorkers
    )
    {
      final Collection<ImmutableWorkerInfo> validWorkers = Collections2.filter(
          zkWorkers,
          ProvisioningUtil.createValidWorkerPredicate(config)
      );
      final Predicate<ImmutableWorkerInfo> isLazyWorker = ProvisioningUtil.createLazyWorkerPredicate(config);
      final int minWorkerCount = workerConfig.getAutoScaler().getMinNumWorkers();
      final int maxWorkerCount = workerConfig.getAutoScaler().getMaxNumWorkers();

      if (minWorkerCount > maxWorkerCount) {
        log.error("Huh? minWorkerCount[%d] > maxWorkerCount[%d]. I give up!", minWorkerCount, maxWorkerCount);
        return;
      }

      if (targetWorkerCount < 0) {
        // Initialize to size of current worker pool, subject to pool size limits
        targetWorkerCount = Math.max(
            Math.min(
                zkWorkers.size(),
                maxWorkerCount
            ),
            minWorkerCount
        );
        log.info(
            "Starting with a target of %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            minWorkerCount,
            maxWorkerCount
        );
      }

      final boolean notTakingActions = currentlyProvisioning.isEmpty()
                                       && currentlyTerminating.isEmpty();
      final boolean shouldScaleUp = notTakingActions
                                    && validWorkers.size() >= targetWorkerCount
                                    && targetWorkerCount < maxWorkerCount
                                    && (hasTaskPendingBeyondThreshold(pendingTasks)
                                        || targetWorkerCount < minWorkerCount);
      final boolean shouldScaleDown = notTakingActions
                                      && validWorkers.size() == targetWorkerCount
                                      && targetWorkerCount > minWorkerCount
                                      && Iterables.any(validWorkers, isLazyWorker);
      if (shouldScaleUp) {
        targetWorkerCount = Math.max(targetWorkerCount + 1, minWorkerCount);
        log.info(
            "I think we should scale up to %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            minWorkerCount,
            maxWorkerCount
        );
      } else if (shouldScaleDown) {
        targetWorkerCount = Math.min(targetWorkerCount - 1, maxWorkerCount);
        log.info(
            "I think we should scale down to %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            minWorkerCount,
            maxWorkerCount
        );
      } else {
        log.info(
            "Our target is %,d workers, and I'm okay with that (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            minWorkerCount,
            maxWorkerCount
        );
      }
    }

    private boolean hasTaskPendingBeyondThreshold(Collection<? extends TaskRunnerWorkItem> pendingTasks)
    {
      long now = System.currentTimeMillis();
      for (TaskRunnerWorkItem pendingTask : pendingTasks) {
        final Duration durationSinceInsertion = new Duration(pendingTask.getQueueInsertionTime().getMillis(), now);
        final Duration timeoutDuration = config.getPendingTaskTimeout().toStandardDuration();
        if (durationSinceInsertion.isEqual(timeoutDuration) || durationSinceInsertion.isLongerThan(timeoutDuration)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public ScalingStats getStats()
    {
      return scalingStats;
    }
  }

}
