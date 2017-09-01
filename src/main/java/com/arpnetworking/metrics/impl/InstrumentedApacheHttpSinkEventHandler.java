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

import com.arpnetworking.metrics.Event;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.Quantity;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * Implementation of {@link ApacheHttpSinkEventHandler} which emits metrics
 * periodically about the performance of the {@link ApacheHttpSink}. Although
 * you cannot extend it through inheritance because it is final, you can
 * extend it by encapsulating it within your own {@link ApacheHttpSinkEventHandler}
 * implementation.
 *
 * TODO(ville): Convert to using PeriodicMetrics from the incubator project.
 * TODO(ville): Add queue length metric by periodically polling the sink.
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
            if (_metrics != null) {
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
            }
        } finally {
            _readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void droppedEvent(final Event event) {
        try {
            _readWriteLock.readLock().lock();
            if (_metrics != null) {
                _metrics.incrementCounter("metrics_client/apache_http_sink/dropped");
            }
        } finally {
            _readWriteLock.readLock().unlock();
        }
    }

    /**
     * Public constructor.
     *
     * @param metricsFactorySupplier {@code Supplier} that provides
     * {@code Optional} {@link MetricsFactory} instance. Decouples the
     * circular reference between this event handler and the metrics factory.
     */
    public InstrumentedApacheHttpSinkEventHandler(final Supplier<Optional<MetricsFactory>> metricsFactorySupplier) {
        _metricsFactorySupplier = metricsFactorySupplier;
        _metrics = _metricsFactorySupplier.get().map(MetricsFactory::create).orElse(null);
        _executorService.scheduleAtFixedRate(
                new PeriodicUnitOfWork(),
                DELAY_IN_MILLISECONDS,
                DELAY_IN_MILLISECONDS,
                TimeUnit.MILLISECONDS);
    }

    private final Supplier<Optional<MetricsFactory>> _metricsFactorySupplier;
    private final ScheduledExecutorService _executorService = Executors.newSingleThreadScheduledExecutor(
            runnable -> new Thread(runnable, "MetricsSinkApacheHttpInstrumention"));

    private final ReadWriteLock _readWriteLock = new ReentrantReadWriteLock();
    private volatile Metrics _metrics;

    private static final long DELAY_IN_MILLISECONDS = 500L;

    private final class PeriodicUnitOfWork implements Runnable {
        @Override
        public void run() {
            if (_metrics != null) {
                try {
                    _readWriteLock.writeLock().lock();
                    _metrics.close();
                    _metrics = null;
                    _metrics = _metricsFactorySupplier.get().map(MetricsFactory::create).orElse(null);
                    if (_metrics != null) {
                        _metrics.resetCounter("metrics_client/apache_http_sink/records");
                        _metrics.resetCounter("metrics_client/apache_http_sink/bytes");
                        _metrics.resetCounter("metrics_client/apache_http_sink/dropped");
                    }
                } finally {
                    _readWriteLock.writeLock().unlock();
                }
            } else {
                _metrics = _metricsFactorySupplier.get().map(MetricsFactory::create).orElse(null);
            }
        }
    }
}
