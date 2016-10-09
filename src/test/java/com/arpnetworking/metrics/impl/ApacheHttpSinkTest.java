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
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tests for {@link ApacheHttpSink}.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ApacheHttpSinkTest {
    @Before
    public void setUp() {
        _wireMockServer = new WireMockServer(0);
        _wireMockServer.start();
        _wireMock = new WireMock(_wireMockServer.port());
    }

    @After
    public void tearDown() {
        _wireMockServer.stop();
    }

    @Test
    public void builderWithDefaults() {
        final Sink sink = new ApacheHttpSink.Builder().build();
        Assert.assertNotNull(sink);
        Assert.assertEquals(ApacheHttpSink.class, sink.getClass());
    }

    @Test
    public void builderWithNulls() {
        final ApacheHttpSink.Builder builder = new ApacheHttpSink.Builder();
        builder.setBufferSize(null);
        builder.setParallelism(null);
        builder.setUri(null);
        final Sink sink = builder.build();
        Assert.assertNotNull(sink);
        Assert.assertEquals(ApacheHttpSink.class, sink.getClass());
    }

    @Test
    public void builderWithInvalidUri() {
        final ApacheHttpSink.Builder builder = new ApacheHttpSink.Builder();
        builder.setUri("not a uri");
        final Sink sink = builder.build();
        Assert.assertNotNull(sink);
        Assert.assertNotEquals(ApacheHttpSink.class, sink.getClass());
    }

    @Test
    public void postSuccess() throws InterruptedException {
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri("http://localhost:" + _wireMockServer.port() + PATH)
//                .setUri("http://localhost:7090" + PATH)
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

        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMock.verifyThat(1, requestPattern);
    }

    @Test
    public void compoundUnits() throws InterruptedException {
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri("http://localhost:" + _wireMockServer.port() + PATH)
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

        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMock.verifyThat(1, requestPattern);
    }

    @Test
    public void noUnits() throws InterruptedException {
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri("http://localhost:" + _wireMockServer.port() + PATH)
                .build();
        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");
        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, null)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, null)),
                createQuantityMap("gauge", TsdQuantity.newInstance(8, null)));
        sink.record(event);

        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMock.verifyThat(1, requestPattern);
    }

    @Test
    public void customUnit() throws InterruptedException {
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri("http://localhost:" + _wireMockServer.port() + PATH)
                .build();
        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");
        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, () -> "Foo")),
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);
        sink.record(event);

        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMock.verifyThat(1, requestPattern);
    }

    @Test
    public void unsupportedBaseUnit() throws InterruptedException {
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri("http://localhost:" + _wireMockServer.port() + PATH)
                .build();
        final Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("foo", "bar");
        final TsdEvent event = new TsdEvent(
                annotations,
                createQuantityMap("timer", TsdQuantity.newInstance(8, Units.ROTATION)),
                createQuantityMap("counter", TsdQuantity.newInstance(8, Units.ROTATION)),
                createQuantityMap("gauge", TsdQuantity.newInstance(8, Units.ROTATION)));
        sink.record(event);

        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMock.verifyThat(1, requestPattern);
    }

    @Test
    public void postFailure() throws InterruptedException {
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(400)));
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri("http://localhost:" + _wireMockServer.port() + PATH)
                .build();
        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);
        sink.record(event);

        Thread.sleep(1000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"));

        // Assert that data was sent
        _wireMock.verifyThat(1, requestPattern);
    }

    @Test
    public void postBadHost() throws InterruptedException {
        final Sink sink = new ApacheHttpSink.Builder()
                .setUri("http://nohost.example.com" + PATH)
                .build();
        final TsdEvent event = new TsdEvent(
                ANNOTATIONS,
                TEST_EMPTY_SERIALIZATION_TIMERS,
                TEST_EMPTY_SERIALIZATION_COUNTERS,
                TEST_EMPTY_SERIALIZATION_GAUGES);
        sink.record(event);
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
                assert samples != null : "first argument must be metric name";
                samples.add((Quantity) argument);
            } else {
                assert false : "unsupported argument type: " + argument.getClass();
            }
        }
        return map;
    }

    private WireMockServer _wireMockServer;
    private WireMock _wireMock;

    private static final String PATH = "/metrics/v1/application";
    private static final Map<String, String> ANNOTATIONS = new LinkedHashMap<>();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_TIMERS = createQuantityMap();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_COUNTERS = createQuantityMap();
    private static final Map<String, List<Quantity>> TEST_EMPTY_SERIALIZATION_GAUGES = createQuantityMap();
}
