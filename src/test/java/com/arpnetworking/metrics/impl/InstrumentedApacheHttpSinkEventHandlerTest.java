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
import com.arpnetworking.metrics.Units;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Semaphore;

/**
 * Tests for {@link InstrumentedApacheHttpSinkEventHandler}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class InstrumentedApacheHttpSinkEventHandlerTest {

    @Test
    public void test() throws InterruptedException {
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

        final ApacheHttpSinkEventHandler eventHandler = new InstrumentedApacheHttpSinkEventHandler(metricsFactory);
        eventHandler.attemptComplete(1, 2, true, TsdQuantity.newInstance(123, Units.NANOSECOND));
        eventHandler.attemptComplete(3, 4, false, TsdQuantity.newInstance(246, Units.NANOSECOND));
        semaphore.acquire();

        Mockito.verify(metricsFactory, Mockito.times(3)).create();
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/records", 1);
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/records", 3);
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/bytes", 2);
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/bytes", 4);
        Mockito.verify(metricsA, Mockito.times(2)).resetCounter("metrics_client/apache_http_sink/success_rate");
        Mockito.verify(metricsA).incrementCounter("metrics_client/apache_http_sink/success_rate");
        Mockito.verify(metricsA).setTimer("metrics_client/apache_http_sink/latency", 123, Units.NANOSECOND);
        Mockito.verify(metricsA).setTimer("metrics_client/apache_http_sink/latency", 246, Units.NANOSECOND);
        Mockito.verify(metricsA).close();
        Mockito.verifyNoMoreInteractions(metricsA);
    }
}
