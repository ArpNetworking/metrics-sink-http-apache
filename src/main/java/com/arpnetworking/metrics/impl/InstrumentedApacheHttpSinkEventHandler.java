/**
 * Copyright 2017 Inscope Metrics, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.metrics.impl;

import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.Quantity;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of {@link ApacheHttpSinkEventHandler} which emits metrics
 * periodically about the performance of the {@link ApacheHttpSink}. Although
 * you cannot extend it through inheritance because it is final, you can
 * extend it by encapsulating it within your own {@link ApacheHttpSinkEventHandler}
 * implementation.
 *
 * TODO(ville): Convert to using PeriodicMetrics from the incubator project.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class InstrumentedApacheHttpSinkEventHandler implements ApacheHttpSinkEventHandler {

    @Override
    public void attemptComplete(
            final long records,
            final long bytes,
            final boolean success,
            final Quantity elapasedTime) {
        try {
            _readWriteLock.readLock().lock();
            _metrics.incrementCounter("metrics_client/apache_http_sink/records", records);
            _metrics.incrementCounter("metrics_client/apache_http_sink/bytes", bytes);
            _metrics.resetCounter("metrics_client/apache_http_sink/success_rate");
            if (success) {
                _metrics.incrementCounter("metrics_client/apache_http_sink/success_rate");
            }
            _metrics.setTimer(
                    "metrics_client/apache_http_sink/latency",
                    elapasedTime.getValue().longValue(),
                    elapasedTime.getUnit());
        } finally {
            _readWriteLock.readLock().unlock();
        }
    }

    /**
     * Public constructor.
     *
     * @param metricsFactory the {@link MetricsFactory} instance.
     */
    public InstrumentedApacheHttpSinkEventHandler(final MetricsFactory metricsFactory) {
        _metricsFactory = metricsFactory;
        _metrics = _metricsFactory.create();
        _executorService.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        final Metrics metrics;
                        try {
                            _readWriteLock.writeLock().lock();
                            metrics = _metrics;
                            _metrics = _metricsFactory.create();
                        } finally {
                            _readWriteLock.writeLock().unlock();
                        }
                        metrics.close();
                    }
                },
                DELAY_IN_MILLISECONDS,
                DELAY_IN_MILLISECONDS,
                TimeUnit.MILLISECONDS);
    }

    private final MetricsFactory _metricsFactory;
    private final ScheduledExecutorService _executorService = Executors.newSingleThreadScheduledExecutor(
            runnable -> new Thread(runnable, "MetricsSinkApacheHttpInstrumention"));

    private final ReadWriteLock _readWriteLock = new ReentrantReadWriteLock();
    private volatile Metrics _metrics;

    private static final long DELAY_IN_MILLISECONDS = 500L;
}
