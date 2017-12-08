/**
 * Copyright 2016 Inscope Metrics, Inc
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
import com.arpnetworking.metrics.Quantity;
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.Units;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.MatchResult;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.ValueMatcher;
import com.inscopemetrics.client.protocol.ClientV2;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Tests for {@link ApacheHttpSink}.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class ApacheHttpSinkTest {

    @Test
    public void testBuilderWithDefaults() {
        final Sink sink = new ApacheHttpSink.Builder().build();
        Assert.assertNotNull(sink);
        Assert.assertEquals(ApacheHttpSink.class, sink.getClass());
    }

    @Test
    public void testBuilderWithNulls() {
        final ApacheHttpSink.Builder builder = new ApacheHttpSink.Builder();
        builder.setBufferSize(null);
        builder.setParallelism(null);
        builder.setUri(null);
        builder.setMaxBatchSize(null);
        builder.setEmptyQueueInterval(null);
        builder.setDispatchErrorLoggingInterval(null);
        builder.setEventsDroppedLoggingInteval(null);
        builder.setEmptyQueueInterval(null);
        final Sink sink = builder.build();
        Assert.assertNotNull(sink);
        Assert.assertEquals(ApacheHttpSink.class, sink.getClass());
    }

    @Test
    public void testBuilderWithHttpsUri() {
        final ApacheHttpSink.Builder builder = new ApacheHttpSink.Builder();
        builder.setUri(URI.create("https://secure.example.com"));
        final Sink sink = builder.build();
        Assert.assertNotNull(sink);
        Assert.assertEquals(ApacheHttpSink.class, sink.getClass());
    }

    @Test
    public void testBuilderWithNotHttpUri() {
        final ApacheHttpSink.Builder builder = new ApacheHttpSink.Builder();
        builder.setUri(URI.create("ftp://ftp.example.com"));
        final Sink sink = builder.build();
        Assert.assertNotNull(sink);
        Assert.assertNotEquals(ApacheHttpSink.class, sink.getClass());
    }

    // CHECKSTYLE.OFF: MethodLength
    @Test
    public void testPostSuccess() throws InterruptedException {
        final String start = Instant.now().minusMillis(812).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT);
        final String end = Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT);
        final String id = UUID.randomUUID().toString();

        _wireMockRule.stubFor(
                WireMock.requestMatching(new RequestValueMatcher(
                        r -> {
                            // Annotations
                            Assert.assertEquals(7, r.getAnnotationsCount());
                            assertAnnotation(r.getAnnotationsList(), "foo", "bar");
                            assertAnnotation(r.getAnnotationsList(), "_service", "myservice");
                            assertAnnotation(r.getAnnotationsList(), "_cluster", "mycluster");
                            assertAnnotation(r.getAnnotationsList(), "_start", start);
                            assertAnnotation(r.getAnnotationsList(), "_end", end);
                            assertAnnotation(r.getAnnotationsList(), "_id", id);

                            // Dimensions
                            Assert.assertEquals(3, r.getDimensionsCount());
                            assertDimension(r.getDimensionsList(), "host", "some.host.com");
                            assertDimension(r.getDimensionsList(), "service", "myservice");
                            assertDimension(r.getDimensionsList(), "cluster", "mycluster");

                            // Samples
                            assertSample(
                                    r.getTimersList(),
                                    "timerLong",
                                    123L,
                                    ClientV2.Unit.Type.Value.SECOND,
                                    ClientV2.Unit.Scale.Value.NANO);
                            assertSample(
                                    r.getTimersList(),
                                    "timerInt",
                                    123,
                                    ClientV2.Unit.Type.Value.SECOND,
                                    ClientV2.Unit.Scale.Value.NANO);
                            assertSample(
                                    r.getTimersList(),
                                    "timerShort",
                                    (short) 123,
                                    ClientV2.Unit.Type.Value.SECOND,
                                    ClientV2.Unit.Scale.Value.NANO);
                            assertSample(
                                    r.getTimersList(),
                                    "timerByte",
                                    (byte) 123,
                                    ClientV2.Unit.Type.Value.SECOND,
                                    ClientV2.Unit.Scale.Value.NANO);
                            assertSample(
                                    r.getCountersList(),
                                    "counter",
                                    8d);
                            assertSample(
                                    r.getGaugesList(),
                                    "gauge",
                                    10d,
                                    ClientV2.Unit.Type.Value.BYTE,
                                    ClientV2.Unit.Scale.Value.UNIT);
                        }))
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)));

        final AtomicBoolean assertionResult = new AtomicBoolean(false);
        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setEventHandler(
                        new AttemptCompletedAssertionHandler(
                                assertionResult,
                                1,
                                451,
                                true,
                                new CompletionHandler(semaphore)))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");
        annotations.put("_start", start);
        annotations.put("_end", end);
        annotations.put("_host", "some.host.com");
        annotations.put("_service", "myservice");
        annotations.put("_cluster", "mycluster");
        annotations.put("_id", id);

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timerLong", TsdQuantity.newInstance(123L, Units.NANOSECOND),
                        "timerInt", TsdQuantity.newInstance(123, Units.NANOSECOND),
                        "timerShort", TsdQuantity.newInstance((short) 123, Units.NANOSECOND),
                        "timerByte", TsdQuantity.newInstance((byte) 123, Units.NANOSECOND)),
                createQuantityMap("counter", TsdQuantity.newInstance(8d, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(10d, Units.BYTE)));

        sink.record(event);
        semaphore.acquire();

        // Ensure expected handler was invoked
        Assert.assertTrue(assertionResult.get());

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }
    // CHECKSTYLE.ON: MethodLength

    @Test
    public void testCompoundUnits() throws InterruptedException {
        _wireMockRule.stubFor(
                WireMock.requestMatching(new RequestValueMatcher(
                        r -> {
                            // Annotations
                            Assert.assertEquals(0, r.getAnnotationsCount());

                            // Dimensions
                            Assert.assertEquals(0, r.getDimensionsCount());

                            // Samples
                            Assert.assertEquals(0, r.getTimersCount());
                            Assert.assertEquals(0, r.getTimersCount());
                            assertSample(
                                    r.getGaugesList(),
                                    "gauge",
                                    10d,
                                    ClientV2.Unit.Type.Value.BIT,
                                    ClientV2.Unit.Scale.Value.UNIT,
                                    ClientV2.Unit.Type.Value.SECOND,
                                    ClientV2.Unit.Scale.Value.UNIT);
                        }))
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)));

        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setEventHandler(new CompletionHandler(semaphore))
                .build();

        final TsdEvent event = new TsdEvent(
                Collections.emptyMap(),
                createQuantityMap(),
                createQuantityMap(),
                createQuantityMap("gauge", TsdQuantity.newInstance(10d, Units.BITS_PER_SECOND)));

        sink.record(event);
        semaphore.acquire();

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testNoUnits() throws InterruptedException {
        _wireMockRule.stubFor(
                WireMock.requestMatching(new RequestValueMatcher(
                        r -> {
                            // Annotations
                            Assert.assertEquals(0, r.getAnnotationsCount());

                            // Dimensions
                            Assert.assertEquals(0, r.getDimensionsCount());

                            // Samples
                            assertSample(
                                    r.getTimersList(),
                                    "timer",
                                    7d);
                            assertSample(
                                    r.getCountersList(),
                                    "counter",
                                    8d);
                            assertSample(
                                    r.getGaugesList(),
                                    "gauge",
                                    9d);
                        }))
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)));

        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setEventHandler(new CompletionHandler(semaphore))
                .build();

        final TsdEvent event = new TsdEvent(
                Collections.emptyMap(),
                createQuantityMap("timer", TsdQuantity.newInstance(7d, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8d, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(9d, null)));

        sink.record(event);
        semaphore.acquire();

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testBatchesRequests() throws InterruptedException {
        _wireMockRule.stubFor(
                WireMock.requestMatching(new RequestValueMatcher(
                        r -> {
                            // Annotations
                            Assert.assertEquals(0, r.getAnnotationsCount());

                            // Dimensions
                            Assert.assertEquals(0, r.getDimensionsCount());

                            // Samples
                            assertSample(
                                    r.getTimersList(),
                                    "timer",
                                    7d);
                            assertSample(
                                    r.getCountersList(),
                                    "counter",
                                    8d);
                            assertSample(
                                    r.getGaugesList(),
                                    "gauge",
                                    9d);
                        }))
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)));

        final AtomicBoolean assertionResult = new AtomicBoolean(false);
        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setMaxBatchSize(10)
                .setParallelism(1)
                .setEmptyQueueInterval(Duration.ofMillis(1000))
                .setEventHandler(
                        new AttemptCompletedAssertionHandler(
                                assertionResult,
                                3,
                                210,
                                true,
                                new CompletionHandler(
                                        semaphore)))
                .build();

        final TsdEvent event = new TsdEvent(
                Collections.emptyMap(),
                createQuantityMap("timer", TsdQuantity.newInstance(7d, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8d, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(9d, null)));

        for (int x = 0; x < 3; x++) {
            sink.record(event);
        }
        semaphore.acquire();

        // Ensure expected handler was invoked
        Assert.assertTrue(assertionResult.get());

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testBatchesRequestsRespectsMax() throws InterruptedException {
        _wireMockRule.stubFor(
                WireMock.requestMatching(new RequestValueMatcher(
                        r -> {
                            // Annotations
                            Assert.assertEquals(0, r.getAnnotationsCount());

                            // Dimensions
                            Assert.assertEquals(0, r.getDimensionsCount());

                            // Samples
                            assertSample(
                                    r.getTimersList(),
                                    "timer",
                                    7d);
                            assertSample(
                                    r.getCountersList(),
                                    "counter",
                                    8d);
                            assertSample(
                                    r.getGaugesList(),
                                    "gauge",
                                    9d);
                        }))
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)));

        final Semaphore semaphore = new Semaphore(-2);
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setMaxBatchSize(2)
                .setParallelism(1)
                .setEmptyQueueInterval(Duration.ofMillis(1000))
                .setEventHandler(new CompletionHandler(semaphore))
                .build();

        final TsdEvent event = new TsdEvent(
                Collections.emptyMap(),
                createQuantityMap("timer", TsdQuantity.newInstance(7d, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8d, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(9d, null)));

        for (int x = 0; x < 5; x++) {
            sink.record(event);
        }
        semaphore.acquire();

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(3, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testRespectsBufferMax() throws InterruptedException {
        final AtomicInteger droppedEvents = new AtomicInteger(0);
        final Semaphore semaphoreA = new Semaphore(0);
        final Semaphore semaphoreB = new Semaphore(0);
        final Semaphore semaphoreC = new Semaphore(-2);
        final AtomicInteger recordsReceived = new AtomicInteger(0);

        _wireMockRule.stubFor(
                WireMock.requestMatching(new RequestValueMatcher(
                        r -> {
                            recordsReceived.incrementAndGet();

                            // Annotations
                            Assert.assertEquals(0, r.getAnnotationsCount());

                            // Dimensions
                            Assert.assertEquals(0, r.getDimensionsCount());

                            // Samples
                            assertSample(
                                    r.getTimersList(),
                                    "timer",
                                    7d);
                            assertSample(
                                    r.getCountersList(),
                                    "counter",
                                    8d);
                            assertSample(
                                    r.getGaugesList(),
                                    "gauge",
                                    9d);
                        }))
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setMaxBatchSize(2)
                .setParallelism(1)
                .setBufferSize(5)
                .setEmptyQueueInterval(Duration.ofMillis(1000))
                .setEventHandler(new RespectsMaxBufferEventHandler(semaphoreA, semaphoreB, semaphoreC, droppedEvents))
                .build();

        final TsdEvent event = new TsdEvent(
                Collections.emptyMap(),
                createQuantityMap("timer", TsdQuantity.newInstance(7d, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8d, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(9d, null)));

        // Add one event to be used as a synchronization point
        sink.record(event);
        semaphoreA.acquire();

        // Add the actual events to analyze
        for (int x = 0; x < 10; x++) {
            sink.record(event);
        }
        semaphoreB.release();
        semaphoreC.acquire();

        // Ensure expected handler was invoked
        Assert.assertEquals(5, droppedEvents.get());

        // Assert number of records received
        Assert.assertEquals(6, recordsReceived.get());

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(4, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testCustomUnit() throws InterruptedException {
        _wireMockRule.stubFor(
                WireMock.requestMatching(new RequestValueMatcher(
                        r -> {
                            // Annotations
                            Assert.assertEquals(0, r.getAnnotationsCount());

                            // Dimensions
                            Assert.assertEquals(0, r.getDimensionsCount());

                            // Samples
                            assertSample(
                                    r.getTimersList(),
                                    "timer",
                                    8d);
                            Assert.assertEquals(0, r.getCountersCount());
                            Assert.assertEquals(0, r.getGaugesCount());
                        }))
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)));

        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setEventHandler(new CompletionHandler(semaphore))
                .build();

        final TsdEvent event = new TsdEvent(
                Collections.emptyMap(),
                createQuantityMap("timer", TsdQuantity.newInstance(8d, () -> "Foo")),
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);
        semaphore.acquire();

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testPostFailure() throws InterruptedException {
        _wireMockRule.stubFor(
                WireMock.requestMatching(new RequestValueMatcher(
                        r -> {
                            // Annotations
                            Assert.assertEquals(0, r.getAnnotationsCount());

                            // Dimensions
                            Assert.assertEquals(0, r.getDimensionsCount());

                            // Samples
                            Assert.assertEquals(0, r.getTimersCount());
                            Assert.assertEquals(0, r.getCountersCount());
                            Assert.assertEquals(0, r.getGaugesCount());
                        }))
                        .willReturn(WireMock.aResponse()
                                .withStatus(400)));

        final AtomicBoolean assertionResult = new AtomicBoolean(false);
        final Semaphore semaphore = new Semaphore(0);
        final org.slf4j.Logger logger = Mockito.mock(org.slf4j.Logger.class);
        final Sink sink = new ApacheHttpSink(
                new ApacheHttpSink.Builder()
                        .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                        .setEventHandler(
                                new AttemptCompletedAssertionHandler(
                                        assertionResult,
                                        1,
                                        2,
                                        false,
                                        new CompletionHandler(
                                                semaphore))),
                logger);

        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);
        semaphore.acquire();

        // Ensure expected handler was invoked
        Assert.assertTrue(assertionResult.get());

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());

        // Assert that an IOException was captured
        Mockito.verify(logger).error(
                Mockito.startsWith("Received failure response when sending metrics to HTTP endpoint; uri="));
    }

    @Test
    public void testPostBadHost() throws InterruptedException {
        final org.slf4j.Logger logger = Mockito.mock(org.slf4j.Logger.class);
        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink(
                new ApacheHttpSink.Builder()
                        .setUri(URI.create("http://nohost.example.com" + PATH))
                        .setEventHandler(new CompletionHandler(semaphore)),
                logger);

        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);
        semaphore.acquire();

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that no data was sent
        _wireMockRule.verify(0, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());

        // Assert that an IOException was captured
        Mockito.verify(logger).error(
                Mockito.startsWith("Encountered failure when sending metrics to HTTP endpoint; uri="),
                Mockito.any(IOException.class));
    }

    @Test
    public void testHttpClientExecuteException() throws InterruptedException {
        final CloseableHttpClient httpClient = Mockito.mock(
                CloseableHttpClient.class,
                invocationOnMock -> {
                    throw new NullPointerException("Throw by default");
                });

        final org.slf4j.Logger logger = Mockito.mock(org.slf4j.Logger.class);
        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink(
                new ApacheHttpSink.Builder()
                        .setUri(URI.create("http://nohost.example.com" + PATH))
                        .setEventHandler(new CompletionHandler(semaphore)),
                () -> httpClient,
                logger);

        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);
        semaphore.acquire();

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that no data was sent
        _wireMockRule.verify(0, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());

        // Assert that the runtime exception was captured
        Mockito.verify(logger).error(
                Mockito.startsWith("Encountered failure when sending metrics to HTTP endpoint; uri="),
                Mockito.any(NullPointerException.class));
    }

    @Test
    public void testHttpClientResponseException() throws InterruptedException, IOException {
        final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
        final CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
        Mockito.doReturn(httpResponse).when(httpClient).execute(Mockito.any(HttpPost.class));
        Mockito.doThrow(new NullPointerException("Throw by default")).when(httpResponse).getStatusLine();

        final org.slf4j.Logger logger = Mockito.mock(org.slf4j.Logger.class);
        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink(
                new ApacheHttpSink.Builder()
                        .setUri(URI.create("http://nohost.example.com" + PATH))
                        .setEventHandler(new CompletionHandler(semaphore)),
                () -> httpClient,
                logger);

        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);
        semaphore.acquire();

        // Verify mocks
        Mockito.verify(httpClient).execute(Mockito.any(HttpPost.class));
        Mockito.verifyNoMoreInteractions(httpClient);
        Mockito.verify(httpResponse).getStatusLine();
        Mockito.verify(httpResponse).close();
        Mockito.verifyNoMoreInteractions(httpResponse);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that no data was sent
        _wireMockRule.verify(0, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());

        // Assert that the runtime exception was captured
        Mockito.verify(logger).error(
                Mockito.startsWith("Encountered failure when sending metrics to HTTP endpoint; uri="),
                Mockito.any(NullPointerException.class));
    }

    @Test
    public void testHttpClientResponseCloseException() throws InterruptedException, IOException {
        final CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);
        final CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
        Mockito.doReturn(httpResponse).when(httpClient).execute(Mockito.any(HttpPost.class));
        Mockito.doThrow(new NullPointerException("Throw by default")).when(httpResponse).getStatusLine();
        Mockito.doThrow(new IllegalStateException("Throw by default")).when(httpResponse).close();

        final org.slf4j.Logger logger = Mockito.mock(org.slf4j.Logger.class);
        final Semaphore semaphore = new Semaphore(0);
        final Sink sink = new ApacheHttpSink(
                new ApacheHttpSink.Builder()
                        .setUri(URI.create("http://nohost.example.com" + PATH))
                        .setEventHandler(new CompletionHandler(semaphore)),
                () -> httpClient,
                logger);

        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);
        semaphore.acquire();

        // Verify mocks
        Mockito.verify(httpClient).execute(Mockito.any(HttpPost.class));
        Mockito.verifyNoMoreInteractions(httpClient);
        Mockito.verify(httpResponse).getStatusLine();
        Mockito.verify(httpResponse).close();
        Mockito.verifyNoMoreInteractions(httpResponse);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that no data was sent
        _wireMockRule.verify(0, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());

        // Assert that the runtime exception was captured
        Mockito.verify(logger).error(
                Mockito.startsWith("Encountered failure when sending metrics to HTTP endpoint; uri="),
                Mockito.any(NullPointerException.class));
        Mockito.verifyNoMoreInteractions(logger);
    }

    @Test
    public void testHttpClientSupplierException() throws InterruptedException, IOException {
        final org.slf4j.Logger logger = Mockito.mock(org.slf4j.Logger.class);
        final Semaphore semaphore = new Semaphore(0);
        final ApacheHttpSinkEventHandler handler = Mockito.mock(ApacheHttpSinkEventHandler.class);
        final Sink sink = new ApacheHttpSink(
                new ApacheHttpSink.Builder()
                        .setUri(URI.create("http://nohost.example.com" + PATH))
                        .setEventHandler(handler),
                () -> {
                    semaphore.release();
                    throw new IllegalStateException("Test Exception");
                },
                logger);

        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);
        semaphore.acquire();

        // Assert that the runtime exception was captured
        Mockito.verify(logger, Mockito.timeout(1000)).error(
                Mockito.startsWith("MetricsSinkApacheHttpWorker failure"),
                Mockito.any(IllegalStateException.class));

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that no data was sent
        _wireMockRule.verify(0, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());

        // Verify no handler was invoked
        Mockito.verify(handler, Mockito.never()).attemptComplete(
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyBoolean(),
                Mockito.any(Quantity.class));
    }

    @Test
    public void testStop() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Semaphore semaphore = new Semaphore(0);
        @SuppressWarnings("unchecked")
        final ApacheHttpSink sink = (ApacheHttpSink) new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setEventHandler(new CompletionHandler(semaphore))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");
        annotations.put("_start", Instant.now().minusMillis(812).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT));
        annotations.put("_end", Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT));
        annotations.put("_host", "some.host.com");
        annotations.put("_service", "myservice");
        annotations.put("_cluster", "mycluster");
        annotations.put("_id", UUID.randomUUID().toString());

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(123, Units.NANOSECOND)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(10, Units.BYTE)));

        sink.stop();
        Thread.sleep(1000);
        sink.record(event);
        Assert.assertFalse(semaphore.tryAcquire(1, TimeUnit.SECONDS));

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMockRule.verify(0, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testNoEventHandler() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");
        annotations.put("_start", Instant.now().minusMillis(812).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT));
        annotations.put("_end", Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_INSTANT));
        annotations.put("_host", "some.host.com");
        annotations.put("_service", "myservice");
        annotations.put("_cluster", "mycluster");
        annotations.put("_id", UUID.randomUUID().toString());

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(123, Units.NANOSECOND)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(10, Units.BYTE)));

        sink.record(event);
    }

    @Test
    public void testSafeSleep() throws InterruptedException {
        final AtomicBoolean value = new AtomicBoolean(false);
        final Thread thread = new Thread(() -> {
            // Value will not be set if safe sleep throws or is not interrupted.
            ApacheHttpSink.safeSleep(500);
            value.set(true);
        });

        thread.start();
        Thread.sleep(100);
        thread.interrupt();
        thread.join(600);

        Assert.assertFalse(thread.isAlive());
        Assert.assertTrue(value.get());
    }

    private static Map<String, List<Quantity>> createQuantityMap(final Object... arguments) {
        // CHECKSTYLE.OFF: IllegalInstantiation - No Guava
        final Map<String, List<Quantity>> map = new HashMap<>();
        // CHECKSTYLE.ON: IllegalInstantiation
        List<Quantity> samples = null;
        for (final Object argument : arguments) {
            if (argument instanceof String) {
                samples = new ArrayList<>();
                map.put((String) argument, samples);
            } else if (argument instanceof Quantity) {
                if (samples == null) {
                    throw new RuntimeException("First argument must be metric name");
                }
                samples.add((Quantity) argument);
            } else {
                throw new RuntimeException("Unsupported argument type: " + argument.getClass());
            }
        }
        return map;
    }

    private static void assertSample(
            final List<ClientV2.MetricEntry> samples,
            final String name,
            final double value) {
        assertSample(samples, name, value, null);
    }

    private void assertSample(
            final List<ClientV2.MetricEntry> samples,
            final String name,
            final double value,
            final ClientV2.Unit.Type.Value numeratorUnitType,
            final ClientV2.Unit.Scale.Value numeratorUnitScale,
            final ClientV2.Unit.Type.Value denominatorUnitType,
            final ClientV2.Unit.Scale.Value denominatorUnitScale) {
        assertSample(
                samples,
                name,
                value,
                ClientV2.CompoundUnit.newBuilder()
                        .addNumerator(
                                0,
                                ClientV2.Unit.newBuilder()
                                        .setScale(numeratorUnitScale)
                                        .setType(numeratorUnitType)
                                        .build())
                        .addDenominator(
                                0,
                                ClientV2.Unit.newBuilder()
                                        .setScale(denominatorUnitScale)
                                        .setType(denominatorUnitType)
                                        .build())
                        .build());
    }

    private static void assertSample(
            final List<ClientV2.MetricEntry> samples,
            final String name,
            final Number value,
            final ClientV2.Unit.Type.Value unitType,
            final ClientV2.Unit.Scale.Value unitScale) {
        assertSample(
                samples,
                name,
                value,
                ClientV2.CompoundUnit.newBuilder()
                        .addNumerator(
                                0,
                                ClientV2.Unit.newBuilder()
                                        .setScale(unitScale)
                                        .setType(unitType)
                                        .build())
                        .build());
    }

    private static void assertSample(
            final List<ClientV2.MetricEntry> samples,
            final String name,
            final Number value,
            @Nullable final ClientV2.CompoundUnit compoundUnit) {

        final ClientV2.Quantity.Builder protobufSample = ClientV2.Quantity.newBuilder();
        if (value instanceof Double || value instanceof Float) {
            protobufSample.setDoubleValue(value.doubleValue());
        } else {
            protobufSample.setLongValue(value.longValue());
        }
        if (compoundUnit != null) {
            protobufSample.setUnit(compoundUnit);
        } else {
            protobufSample.setUnit(ClientV2.CompoundUnit.newBuilder().build());
        }

        Assert.assertTrue(
                String.format("Missing sample: name=%s, value=%s (%s)", name, value, value.getClass().getName()),
                samples.contains(
                        ClientV2.MetricEntry.newBuilder()
                                .setName(name)
                                .addSamples(
                                        0,
                                        protobufSample.build())
                                .build()));
    }

    private static void assertAnnotation(
            final List<ClientV2.AnnotationEntry> annotations,
            final String name,
            final String value) {
        Assert.assertTrue(annotations.contains(
                ClientV2.AnnotationEntry.newBuilder()
                        .setName(name)
                        .setValue(value)
                        .build()));
    }

    private static void assertDimension(
            final List<ClientV2.DimensionEntry> dimensions,
            final String name,
            final String value) {
        Assert.assertTrue(dimensions.contains(
                ClientV2.DimensionEntry.newBuilder()
                        .setName(name)
                        .setValue(value)
                        .build()));
    }

    @Rule
    public WireMockRule _wireMockRule = new WireMockRule(
            WireMockConfiguration.wireMockConfig().dynamicPort());

    private static final String PATH = "/metrics/v1/application";
    private static final Map<String, String> ANNOTATIONS = new LinkedHashMap<>();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_TIMERS = createQuantityMap();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_COUNTERS = createQuantityMap();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_GAUGES = createQuantityMap();

    private static final class RespectsMaxBufferEventHandler implements ApacheHttpSinkEventHandler {
        private final Semaphore _semaphoreA;
        private final Semaphore _semaphoreB;
        private final Semaphore _semaphoreC;
        private final AtomicInteger _droppedEvents;

        /* package private */ RespectsMaxBufferEventHandler(
                final Semaphore semaphoreA,
                final Semaphore semaphoreB,
                final Semaphore semaphoreC,
                final AtomicInteger droppedEvents) {
            _semaphoreA = semaphoreA;
            _semaphoreB = semaphoreB;
            _semaphoreC = semaphoreC;
            _droppedEvents = droppedEvents;
        }

        @Override
        public void attemptComplete(
                final long records,
                final long bytes,
                final boolean success,
                final Quantity elapasedTime) {
            if (_semaphoreA.availablePermits() == 0) {
                // Initial synchronization request (leave a an extra permit to mark its completion)
                _semaphoreA.release(2);
                try {
                    _semaphoreB.acquire();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                // Actual buffered requests
                _semaphoreC.release();
            }
        }

        @Override
        public void droppedEvent(final Event event) {
            _droppedEvents.incrementAndGet();
        }
    }

    private static final class CompletionHandler implements ApacheHttpSinkEventHandler {

        private final Semaphore _semaphore;
        private final Optional<ApacheHttpSinkEventHandler> _nextHandler;

        /* package private */ CompletionHandler(final Semaphore semaphore) {
            _semaphore = semaphore;
            _nextHandler = Optional.empty();
        }

        /* package private */ CompletionHandler(
                final Semaphore semaphore,
                final ApacheHttpSinkEventHandler nextHandler) {
            _semaphore = semaphore;
            _nextHandler = Optional.of(nextHandler);
        }

        @Override
        public void attemptComplete(
                final long records,
                final long bytes,
                final boolean success,
                final Quantity elapasedTime) {
            _semaphore.release();
            _nextHandler.ifPresent(h -> h.attemptComplete(records, bytes, success, elapasedTime));
        }

        @Override
        public void droppedEvent(final Event event) {
            _nextHandler.ifPresent(h -> h.droppedEvent(event));
        }
    }

    private static final class AttemptCompletedAssertionHandler implements ApacheHttpSinkEventHandler {

        private final AtomicBoolean _assertionResult;
        private final long _expectedRecords;
        private final long _expectedBytes;
        private final boolean _expectedSuccess;
        private final Optional<ApacheHttpSinkEventHandler> _nextHandler;

        /* package private */ AttemptCompletedAssertionHandler(
                final AtomicBoolean assertionResult,
                final long expectedRecords,
                final long expectedBytes,
                final boolean expectedSuccess) {
            _assertionResult = assertionResult;
            _expectedRecords = expectedRecords;
            _expectedBytes = expectedBytes;
            _expectedSuccess = expectedSuccess;
            _nextHandler = Optional.empty();
        }

        /* package private */ AttemptCompletedAssertionHandler(
                final AtomicBoolean assertionResult,
                final long expectedRecords,
                final long expectedBytes,
                final boolean expectedSuccess,
                final ApacheHttpSinkEventHandler nextHandler) {
            _assertionResult = assertionResult;
            _expectedRecords = expectedRecords;
            _expectedBytes = expectedBytes;
            _expectedSuccess = expectedSuccess;
            _nextHandler = Optional.of(nextHandler);
        }

        @Override
        public void attemptComplete(
                final long records,
                final long bytes,
                final boolean success,
                final Quantity elapasedTime) {
            try {
                Assert.assertEquals(_expectedRecords, records);
                Assert.assertEquals(_expectedBytes, bytes);
                Assert.assertEquals(_expectedSuccess, success);
                _assertionResult.set(true);
                // CHECKSTYLE.OFF: IllegalCatch - JUnit throws assertions which derive from Throwable
            } catch (final Throwable t) {
                // CHECKSTYLE.ON: IllegalCatch
                _assertionResult.set(false);
            }
            _nextHandler.ifPresent(h -> h.attemptComplete(records, bytes, success, elapasedTime));
        }

        @Override
        public void droppedEvent(final Event event) {
            _nextHandler.ifPresent(h -> h.droppedEvent(event));
        }
    }

    private static final class EventDroppedAssertionHandler implements ApacheHttpSinkEventHandler {

        private final AtomicInteger _droppedCount;
        private final Optional<ApacheHttpSinkEventHandler> _nextHandler;

        /* package private */ EventDroppedAssertionHandler(final AtomicInteger droppedCount) {
            _droppedCount = droppedCount;
            _nextHandler = Optional.empty();
        }

        /* package private */ EventDroppedAssertionHandler(
                final AtomicInteger droppedCount,
                final ApacheHttpSinkEventHandler nextHandler) {
            _droppedCount = droppedCount;
            _nextHandler = Optional.of(nextHandler);
        }

        @Override
        public void attemptComplete(
                final long records,
                final long bytes,
                final boolean success,
                final Quantity elapasedTime) {
            _nextHandler.ifPresent(h -> h.attemptComplete(records, bytes, success, elapasedTime));
        }

        @Override
        public void droppedEvent(final Event event) {
            _droppedCount.incrementAndGet();
            _nextHandler.ifPresent(h -> h.droppedEvent(event));
        }
    }

    private static class RequestValueMatcher implements ValueMatcher<Request> {

        private final Consumer<ClientV2.Record> _validator;

        /* package private */ RequestValueMatcher(final Consumer<ClientV2.Record> validator) {
            _validator = validator;
        }

        @Override
        public MatchResult match(final Request request) {
            if (!request.getMethod().equals(RequestMethod.POST)) {
                System.out.println(String.format(
                        "Request method does not match; expect=%s, actual=%s",
                        RequestMethod.POST,
                        request.getMethod()));
                return MatchResult.noMatch();
            }
            if (!request.getUrl().equals(PATH)) {
                System.out.println(String.format(
                        "Request uri does not match; expect=%s, actual=%s",
                        PATH,
                        request.getUrl()));
                return MatchResult.noMatch();
            }
            try {
                final ClientV2.RecordSet recordSet = ClientV2.RecordSet.parseFrom(request.getBody());
                recordSet.getRecordsList().forEach(_validator::accept);
                return MatchResult.of(true);
                // CHECKSTYLE.OFF: IllegalCatch - JUnit throws assertions which derive from Throwable
            } catch (final Throwable t) {
                // CHECKSTYLE.ON: IllegalCatch
                System.out.println("Error parsing body: " + t);
                t.printStackTrace();
                return MatchResult.noMatch();
            }
        }
    }
}
