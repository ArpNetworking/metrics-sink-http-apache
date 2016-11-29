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

import com.arpnetworking.metrics.Quantity;
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.Units;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for {@link ApacheHttpSink}.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
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

    @Test
    public void testPostSuccess() throws InterruptedException {
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

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testCompoundUnits() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(123, Units.NANOSECOND),
                        "minutes", TsdQuantity.newInstance(5, Units.MINUTE),
                        "hours", TsdQuantity.newInstance(5, Units.HOUR),
                        "day", TsdQuantity.newInstance(5, Units.DAY),
                        "week", TsdQuantity.newInstance(5, Units.WEEK)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, Units.BYTE)),
                createQuantityMap("gauge", TsdQuantity.newInstance(10, Units.BITS_PER_SECOND),
                        "hdd_c", TsdQuantity.newInstance(40, Units.CELSIUS),
                        "hdd_k", TsdQuantity.newInstance(400, Units.KELVIN),
                        "hdd_f", TsdQuantity.newInstance(100, Units.FAHRENHEIT)));

        sink.record(event);

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testNoUnits() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(8, null)));

        sink.record(event);

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testBatchesRequests() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setMaxBatchSize(10)
                .setParallelism(1)
                .setEmptyQueueInterval(Duration.ofMillis(1000))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(8, null)));

        // TODO(ville): This is trying to synchronize the two sleeps - WTF.
        // This use case is likely best served with a sleeper much like in MAD.
        Thread.sleep(500);
        for (int x = 0; x < 3; x++) {
            sink.record(event);
        }

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testBatchesRequestsRespectsMax() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setMaxBatchSize(2)
                .setParallelism(1)
                .setEmptyQueueInterval(Duration.ofMillis(1000))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(8, null)));

        // TODO(ville): This is trying to synchronize the two sleeps - WTF.
        // This use case is likely best served with a sleeper much like in MAD.
        Thread.sleep(500);
        for (int x = 0; x < 5; x++) {
            sink.record(event);
        }

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(3, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testRespectsBufferMax() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .setMaxBatchSize(2)
                .setParallelism(1)
                .setBufferSize(5)
                .setEmptyQueueInterval(Duration.ofMillis(1000))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(8, null)));

        // TODO(ville): This is trying to synchronize the two sleeps - WTF.
        // This use case is likely best served with a sleeper much like in MAD.
        Thread.sleep(500);
        for (int x = 0; x < 10; x++) {
            sink.record(event);
        }

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(3, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testCustomUnit() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, () -> "Foo")),
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testUnsupportedBaseUnit() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .build();

        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");

        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, Units.ROTATION)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, Units.ROTATION)),
                createQuantityMap("gauge", TsdQuantity.newInstance(8, Units.ROTATION)));

        sink.record(event);

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());
    }

    @Test
    public void testPostFailure() throws InterruptedException {
        _wireMockRule.stubFor(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(400)));

        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://localhost:" + _wireMockRule.port() + PATH))
                .build();

        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);

        sink.record(event);

        // TODO(ville): Replace this with a trigger, logical logic, etc.
        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        //TODO(ville): Actually assert that the expected data was sent.

        // Assert that data was sent
        _wireMockRule.verify(1, requestPattern);
        Assert.assertTrue(_wireMockRule.findUnmatchedRequests().getRequests().isEmpty());

        //TODO(ville): Actually assert that the failure was handled; e.g. log event.
    }

    @Test
    public void testPostBadHost() throws InterruptedException {
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri(URI.create("http://nohost.example.com" + PATH))
                .build();
        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);
        sink.record(event);

        // TODO(ville): There should be some record of failure; we should assert that.
        // So with retries the bad request would go around a few times and then get
        // dropped. When it does an injected logger should spout a warning. That's the
        // only effect we have of this case executing properly, and we should assert
        // that (much as we do in the metrics client java).
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

    @Rule
    public WireMockRule _wireMockRule = new WireMockRule(
            WireMockConfiguration.wireMockConfig().dynamicPort());

    private static final String PATH = "/metrics/v1/application";
    private static final Map<String, String> ANNOTATIONS = new LinkedHashMap<>();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_TIMERS = createQuantityMap();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_COUNTERS = createQuantityMap();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_GAUGES = createQuantityMap();
}
