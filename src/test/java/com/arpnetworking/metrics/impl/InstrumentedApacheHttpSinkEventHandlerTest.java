/*
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
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link InstrumentedApacheHttpSinkEventHandler}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class InstrumentedApacheHttpSinkEventHandlerTest {

    @Test
    public void testRecordCompletedAttempts() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(-2);

        final MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
        final Metrics metricsA = Mockito.mock(Metrics.class, "A");
        final Metrics metricsB = Mockito.mock(Metrics.class, "B");

        Mockito.doAnswer(ignored -> {
            try {
                if (semaphore.availablePermits() == -2) {
                    return metricsA;
                } else {
                    return metricsB;
                }
            } finally {
                semaphore.release();
            }
        }).when(metricsFactory).create();

        final ApacheHttpSinkEventHandler eventHandler = new InstrumentedApacheHttpSinkEventHandler(
                () -> Optional.of(metricsFactory));
        eventHandler.attemptComplete(1, 2, true, 123, TimeUnit.NANOSECONDS);
        eventHandler.attemptComplete(3, 4, false, 246, TimeUnit.NANOSECONDS);
        semaphore.acquire();

        Mockito.verify(metricsFactory, Mockito.times(3)).create();
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/records", 1);
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/records", 3);
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/bytes", 2);
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/bytes", 4);
        Mockito.verify(metricsA, Mockito.times(2)).resetCounter("metrics_client/apache_http_sink/success_rate");
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/success_rate");
        Mockito.verify(metricsA).setTimer("metrics_client/apache_http_sink/latency", 123, TimeUnit.NANOSECONDS);
        Mockito.verify(metricsA).setTimer("metrics_client/apache_http_sink/latency", 246, TimeUnit.NANOSECONDS);
        Mockito.verify(metricsA).close();
        Mockito.verifyNoMoreInteractions(metricsA);
    }

    @Test
    public void testRecordCompletedAttemptsMetricsThrows() throws InterruptedException {
        // NOTE: The metrics instance is exception safe by contract; this
        // unit test exists only to work around deficiencies in Jacoco.
        // See: https://github.com/jacoco/jacoco/issues/15
        final Semaphore semaphore = new Semaphore(0);

        final MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
        final Metrics metrics = Mockito.mock(Metrics.class);
        Mockito.doAnswer(ignored -> {
                    semaphore.release();
                    return metrics;
                })
                .doAnswer(ignored -> {
                    semaphore.release();
                    return null;
                })
                .when(metricsFactory)
                .create();
        Mockito.doThrow(new IllegalStateException("Test Exception"))
                .when(metrics)
                .incrementCounter(Mockito.anyString(), Mockito.anyLong());

        final ApacheHttpSinkEventHandler eventHandler = new InstrumentedApacheHttpSinkEventHandler(
                () -> Optional.of(metricsFactory));

        semaphore.acquire();
        try {
            eventHandler.attemptComplete(3, 4, false, 246, TimeUnit.NANOSECONDS);
        } catch (final IllegalStateException e) {
            // Ignore
        }
        semaphore.acquire();

        // The following is used to force the write lock to be given up; since
        // the second metrics instance created by our factory is null this data
        // is not recorded.
        eventHandler.attemptComplete(-1, -1, true, 123, TimeUnit.NANOSECONDS);

        Mockito.verify(metricsFactory, Mockito.times(2)).create();
        Mockito.verify(metrics).incrementCounter("metrics_client/apache_http_sink/records", 3);
        Mockito.verify(metrics).close();
        Mockito.verifyNoMoreInteractions(metrics);
    }

    @Test
    public void testRecordDropped() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(-2);

        final Event eventA = Mockito.mock(Event.class, "A");
        final Event eventB = Mockito.mock(Event.class, "B");
        final MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
        final Metrics metricsA = Mockito.mock(Metrics.class, "A");
        final Metrics metricsB = Mockito.mock(Metrics.class, "B");

        Mockito.doAnswer(ignored -> {
            try {
                if (semaphore.availablePermits() == -2) {
                    return metricsA;
                } else {
                    return metricsB;
                }
            } finally {
                semaphore.release();
            }
        }).when(metricsFactory).create();

        final ApacheHttpSinkEventHandler eventHandler = new InstrumentedApacheHttpSinkEventHandler(
                () -> Optional.of(metricsFactory));
        eventHandler.droppedEvent(eventA);
        eventHandler.droppedEvent(eventB);
        semaphore.acquire();

        Mockito.verify(metricsFactory, Mockito.times(3)).create();
        Mockito.verify(metricsA, Mockito.times(2)).incrementCounter("metrics_client/apache_http_sink/dropped");
        Mockito.verify(metricsA).close();
        Mockito.verifyNoMoreInteractions(metricsA);
    }

    @Test
    public void testRecordDroppedMetricsThrows() throws InterruptedException {
        // NOTE: The metrics instance is exception safe by contract; this
        // unit test exists only to work around deficiencies in Jacoco.
        // See: https://github.com/jacoco/jacoco/issues/15
        final Semaphore semaphore = new Semaphore(0);

        final Event eventA = Mockito.mock(Event.class, "A");
        final Event eventB = Mockito.mock(Event.class, "B");
        final MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
        final Metrics metrics = Mockito.mock(Metrics.class);
        Mockito.doAnswer(ignored -> {
            semaphore.release();
            return metrics;
        })
                .doAnswer(ignored -> {
                    semaphore.release();
                    return null;
                })
                .when(metricsFactory)
                .create();
        Mockito.doThrow(new IllegalStateException("Test Exception"))
                .when(metrics)
                .incrementCounter(Mockito.anyString());

        final ApacheHttpSinkEventHandler eventHandler = new InstrumentedApacheHttpSinkEventHandler(
                () -> Optional.of(metricsFactory));

        semaphore.acquire();
        try {
            eventHandler.droppedEvent(eventA);
        } catch (final IllegalStateException e) {
            // Ignore
        }
        semaphore.acquire();

        // The following is used to force the write lock to be given up; since
        // the second metrics instance created by our factory is null this data
        // is not recorded.
        eventHandler.droppedEvent(eventB);

        Mockito.verify(metricsFactory, Mockito.times(2)).create();
        Mockito.verify(metrics).incrementCounter("metrics_client/apache_http_sink/dropped");
        Mockito.verify(metrics).close();
        Mockito.verifyNoMoreInteractions(metrics);
    }

    @Test
    public void testMetricsFactoryThrows() throws InterruptedException {
        // NOTE: The metrics factory instance is exception safe by contract;
        // this unit test exists only to work around deficiencies in Jacoco.
        // See: https://github.com/jacoco/jacoco/issues/15
        final Semaphore semaphore = new Semaphore(0);

        final MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
        final Metrics metrics = Mockito.mock(Metrics.class);
        Mockito.doAnswer(ignored -> {
            semaphore.release();
            return metrics;
        })
                .doAnswer(ignored -> {
                    semaphore.release();
                    throw new IllegalStateException("Test Exception");
                })
                .when(metricsFactory)
                .create();

        final ApacheHttpSinkEventHandler eventHandler = new InstrumentedApacheHttpSinkEventHandler(
                () -> Optional.of(metricsFactory));

        semaphore.acquire();
        eventHandler.attemptComplete(3, 4, false, 246, TimeUnit.NANOSECONDS);
        semaphore.acquire();

        // The following is used to force the write lock to be given up; since
        // the second metrics instance created by our factory is null this data
        // is not recorded.
        eventHandler.attemptComplete(-1, -1, true, 123, TimeUnit.NANOSECONDS);

        Mockito.verify(metricsFactory, Mockito.times(2)).create();
        Mockito.verify(metrics).incrementCounter("metrics_client/apache_http_sink/records", 3);
        Mockito.verify(metrics).incrementCounter("metrics_client/apache_http_sink/bytes", 4);
        Mockito.verify(metrics, Mockito.times(1)).resetCounter("metrics_client/apache_http_sink/success_rate");
        Mockito.verify(metrics).setTimer("metrics_client/apache_http_sink/latency", 246, TimeUnit.NANOSECONDS);
        Mockito.verify(metrics).close();
        Mockito.verifyNoMoreInteractions(metrics);
    }

    @Test
    public void testSkipUntilMetricsFactoryAvailable() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(-1);

        final MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
        final Metrics metricsA = Mockito.mock(Metrics.class, "A");

        Mockito.doAnswer(ignored -> {
            semaphore.release();
            return metricsA;
        }).when(metricsFactory).create();

        final ApacheHttpSinkEventHandler eventHandler = new InstrumentedApacheHttpSinkEventHandler(() -> {
            if (semaphore.availablePermits() == -1) {
                semaphore.release();
                return Optional.empty();
            }
            return Optional.of(metricsFactory);
        });
        eventHandler.attemptComplete(1, 2, true, 123, TimeUnit.NANOSECONDS);
        semaphore.acquire();
        eventHandler.attemptComplete(3, 4, false, 246, TimeUnit.NANOSECONDS);
        semaphore.acquire();

        Mockito.verify(metricsFactory, Mockito.times(2)).create();
        Mockito.verify(metricsA).resetCounter("metrics_client/apache_http_sink/records");
        Mockito.verify(metricsA).resetCounter("metrics_client/apache_http_sink/bytes");
        Mockito.verify(metricsA).resetCounter("metrics_client/apache_http_sink/dropped");
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/records", 3);
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/bytes", 4);
        Mockito.verify(metricsA, Mockito.times(1)).resetCounter("metrics_client/apache_http_sink/success_rate");
        Mockito.verify(metricsA).setTimer("metrics_client/apache_http_sink/latency", 246, TimeUnit.NANOSECONDS);
        Mockito.verify(metricsA).close();
        Mockito.verifyNoMoreInteractions(metricsA);
    }
}
