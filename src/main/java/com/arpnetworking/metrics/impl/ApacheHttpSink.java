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

import com.arpnetworking.metrics.CompoundUnit;
import com.arpnetworking.metrics.Event;
import com.arpnetworking.metrics.Quantity;
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.Unit;
import com.google.protobuf.ByteString;
import com.inscopemetrics.client.protocol.ClientV1;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;

/**
 * Http sink using the Protobuf format for metrics and the Apache HTTP library.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class ApacheHttpSink implements Sink {
    @Override
    public void record(final Event event) {
        _events.push(event);
        final int events = _eventsCount.incrementAndGet();
        if (events > _bufferSize) {
            _events.pollFirst();
        }
    }

    static void uninterruptableSleep(final int millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
        }
    }

    private ApacheHttpSink(final Builder builder) {
        //TODO(brandon): Limit the number of work units buffered
        final ExecutorService executor = Executors.newFixedThreadPool(
                builder._parallelism,
                (runnable) -> new Thread(runnable, "ApacheHttpSinkWorker"));
        final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setDefaultMaxPerRoute(builder._parallelism);
        connectionManager.setMaxTotal(builder._parallelism);
        _httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();
        _uri = builder._uri;
        _bufferSize = builder._bufferSize;
        _maxBatchSize = builder._maxBatchSize;
        _sleepMillis = builder._emptyQueueSleepMillis;
        for (int i = 0; i < builder._parallelism; i++) {
            executor.execute(new HttpDispatch());
        }
    }

    private final CloseableHttpClient _httpClient;
    private final String _uri;
    private final int _bufferSize;
    private final int _maxBatchSize;
    private final int _sleepMillis;
    private final ConcurrentLinkedDeque<Event> _events = new ConcurrentLinkedDeque<>();
    private final AtomicInteger _eventsCount = new AtomicInteger(0);
//    private final AtomicBoolean _run = new AtomicBoolean(true);

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ApacheHttpSink.class);

    private final class HttpDispatch implements Runnable {
        @Override
        public void run() {
            ClientV1.RecordSet.Builder requestBuilder = ClientV1.RecordSet.newBuilder();
            while (true) {
                // Collect a set of events to send
                // NOTE: This also builds the serialization types in order to spend more time
                // allowing more records to arrive in the batch
                int collected = 0;

                Event event;
                do {
                    event = _events.pollFirst();
                    if (event == null) {
                        break;
                    }

                    _eventsCount.decrementAndGet();
                    collected++;
                    requestBuilder.addRecords(serializeEvent(event));
                } while (collected < _maxBatchSize);

                if (collected > 0) {
                    final ClientV1.RecordSet recordSet = requestBuilder.build();
                    dispatchRequest(recordSet);

                    // Prepare the next request builder
                    requestBuilder = ClientV1.RecordSet.newBuilder();
                }

                // Sleep if we didn't max out the batch size
                if (event == null) {
                    uninterruptableSleep(_sleepMillis);
                }
            }
        }

        private void dispatchRequest(final ClientV1.RecordSet recordSet) {
            final HttpPost post = new HttpPost(_uri);

            post.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/octet-stream"));
            post.setEntity(new ByteArrayEntity(recordSet.toByteArray()));

            try (final CloseableHttpResponse response = _httpClient.execute(post)) {
                final int statusCode = response.getStatusLine().getStatusCode();
                if ((statusCode / 100) != 2) {
                    LOGGER.error(
                            String.format("Received non 200 response when sending metrics to HTTP endpoint; uri=%s, status=%s",
                                    _uri,
                                    statusCode));
                }
            } catch (final IOException e) {
                LOGGER.error(String.format("Unable to send metrics to HTTP endpoint; uri=%s", _uri), e);
            }
        }

        private ClientV1.Record serializeEvent(final Event event) {
            final ClientV1.Record.Builder builder = ClientV1.Record.newBuilder();
            for (Map.Entry<String, String> annotation : event.getAnnotations().entrySet()) {
                builder.addAnnotations(
                        ClientV1.AnnotationEntry.newBuilder()
                                .setName(annotation.getKey())
                                .setValue(annotation.getValue())
                                .build());
            }

            for (Map.Entry<String, List<Quantity>> entry : event.getCounterSamples().entrySet()) {
                builder.addCounters(ClientV1.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            for (Map.Entry<String, List<Quantity>> entry : event.getTimerSamples().entrySet()) {
                builder.addTimers(ClientV1.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            for (Map.Entry<String, List<Quantity>> entry : event.getGaugeSamples().entrySet()) {
                builder.addGauges(ClientV1.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            //TODO(brandonarp): just pull from dimensions once the library supports it
            final String host = event.getAnnotations().get("_host");
            if (host != null) {
                builder.addDimensions(ClientV1.DimensionEntry.newBuilder().setName("host").setValue(host).build());
            }

            final String service = event.getAnnotations().get("_service");
            if (service != null) {
                builder.addDimensions(ClientV1.DimensionEntry.newBuilder().setName("service").setValue(service).build());
            }

            final String cluster = event.getAnnotations().get("_cluster");
            if (cluster != null) {
                builder.addDimensions(ClientV1.DimensionEntry.newBuilder().setName("cluster").setValue(cluster).build());
            }

            extractTimestamp(event, "_end", builder::setEndMillisSinceEpoch);
            extractTimestamp(event, "_start", builder::setStartMillisSinceEpoch);

            final String id = event.getAnnotations().get("_id");
            if (id != null) {
                final UUID uuid = UUID.fromString(id);
                final ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
                buffer.putLong(uuid.getMostSignificantBits());
                buffer.putLong(uuid.getLeastSignificantBits());
                buffer.rewind();
                builder.setId(ByteString.copyFrom(buffer));
            }
            return builder.build();
        }

        private void extractTimestamp(final Event event, final String key, final LongConsumer setter) {
            final String endTimestamp = event.getAnnotations().get(key);
            if (endTimestamp != null) {
                final ZonedDateTime time = ZonedDateTime.parse(endTimestamp);
                setter.accept(time.toInstant().toEpochMilli());
            }
        }

        private List<ClientV1.DoubleQuantity> buildQuantities(final List<Quantity> quantities) {
            final List<ClientV1.DoubleQuantity> timerQuantities = new ArrayList<>(quantities.size());
            for (Quantity quantity : quantities) {
                final ClientV1.DoubleQuantity.Builder quantityBuilder = ClientV1.DoubleQuantity.newBuilder()
                        .setValue(quantity.getValue().doubleValue());
                final ClientV1.CompoundUnit unit = buildUnit(quantity.getUnit());
                if (unit != null) {
                    quantityBuilder.setUnit(unit);
                }
                timerQuantities.add(quantityBuilder.build());
            }
            return timerQuantities;
        }

        @Nullable
        private ClientV1.CompoundUnit buildUnit(final Unit unit) {
            if (unit == null) {
                return null;
            } else {
                final ClientV1.CompoundUnit.Builder builder = ClientV1.CompoundUnit.newBuilder();
                if (unit instanceof CompoundUnit) {
                    final CompoundUnit compoundUnit = (CompoundUnit) unit;
                    for (Unit numeratorUnit : compoundUnit.getNumeratorUnits()) {
                        builder.addNumerator(mapUnit(numeratorUnit));
                    }

                    for (Unit denominatorUnit : compoundUnit.getDenominatorUnits()) {
                        builder.addDenominator(mapUnit(denominatorUnit));
                    }
                } else {
                    final ClientV1.Unit numeratorUnit = mapUnit(unit);
                    if (numeratorUnit != null) {
                        builder.addNumerator(numeratorUnit);
                    }
                }
                return builder.build();
            }
        }

        private ClientV1.Unit mapUnit(final Unit unit) {
            final BaseUnit baseUnit;
            final BaseScale baseScale;
            if (unit instanceof TsdUnit) {
                final TsdUnit tsdUnit = (TsdUnit) unit;
                baseUnit = tsdUnit.getBaseUnit();
                baseScale = tsdUnit.getBaseScale();
            } else if (unit instanceof BaseUnit) {
                baseUnit = (BaseUnit) unit;
                baseScale = null;
            } else {
                return null;
            }

            final ClientV1.Unit.Builder builder = ClientV1.Unit.newBuilder();
            builder.setType(ClientV1.Unit.Type.valueOf(baseUnit.name()));
            if (baseScale != null) {
                builder.setScale(ClientV1.Unit.Scale.valueOf(baseScale.name()));
            } else {
                builder.setScale(ClientV1.Unit.Scale.UNIT);
            }

            return builder.build();
        }
    }

    /**
     * Builder for {@link ApacheHttpSink}.
     *
     * This class is thread safe.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class Builder {

        /**
         * Set the buffer size in number of events. Optional; default is 10,000.
         *
         * @param value The number of events to buffer.
         * @return This {@link Builder} instance.
         */
        public Builder setBufferSize(final Integer value) {
            _bufferSize = value;
            return this;
        }

        /**
         * Set the URI of the HTTP endpoint. Optional; default is "http://localhost:7090/metrics/v1/application".
         *
         * @param value The uri of the HTTP endpoint.
         * @return This {@link Builder} instance.
         */
        public Builder setUri(final String value) {
            _uri = value;
            return this;
        }

        /**
         * Set the parallelism (threads and connections) that the sink will use. Optional; default is 5.
         *
         * @param value The uri of the HTTP endpoint.
         * @return This {@link Builder} instance.
         */
        public Builder setParallelism(final Integer value) {
            _parallelism = value;
            return this;
        }

        /**
         * Set the empty queue sleep duration, in milliseconds. Optional; default is 500.
         *
         * @param value The sleep time.
         * @return This {@link Builder} instance.
         */
        public Builder setEmptyQueueSleepMillis(final Integer value) {
            _emptyQueueSleepMillis = value;
            return this;
        }

        /**
         * Set the maximum batch size (records to send in each request). Optional; default is 500.
         *
         * @param value The maximum batch size.
         * @return This {@link Builder} instance.
         */
        public Builder setMaxBatchSize(final Integer value) {
            _maxBatchSize = value;
            return this;
        }

        /**
         * Create an instance of {@link Sink}.
         *
         * @return Instance of {@link Sink}.
         */
        public Sink build() {
            // Defaults
            applyDefaults();

            // Validate
            final List<String> failures = new ArrayList<>();
            validate(failures);

            // Fallback
            if (!failures.isEmpty()) {
                LOGGER.warn(String.format(
                        "Unable to construct %s, metrics disabled; failures=%s",
                        this.getClass().getEnclosingClass().getSimpleName(),
                        failures));
                return new WarningSink(failures);
            }

            return new ApacheHttpSink(this);
        }

        /**
         * Protected method allows child builder classes to add additional
         * defaulting behavior to fields.
         */
        private void applyDefaults() {
            if (_bufferSize == null) {
                _bufferSize = DEFAULT_BUFFER_SIZE;
            }
            if (_uri == null) {
                _uri = DEFAULT_URI;
            }
            if (_parallelism == null) {
                _parallelism = DEFAULT_PARALLELISM;
            }
            if (_maxBatchSize == null) {
                _maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
            }
            if (_emptyQueueSleepMillis == null) {
                _emptyQueueSleepMillis = DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS;
            }
        }

        /**
         * Validates the builder.  Adds failure messages to the failures parameter on failure.
         *
         * @param failures List of validation failures.
         */
        private void validate(final List<String> failures) {
            if (!_uri.contains("http")) {
                failures.add(String.format("URI must be an http(s) URI; uri=%s", _uri));
            }
        }

        private Integer _bufferSize = DEFAULT_BUFFER_SIZE;
        private String _uri = DEFAULT_URI;
        private Integer _parallelism = DEFAULT_PARALLELISM;
        private Integer _maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
        private Integer _emptyQueueSleepMillis = DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS;

        private static final Integer DEFAULT_BUFFER_SIZE = 10000;
        private static final Integer DEFAULT_MAX_BATCH_SIZE = 500;
        private static final Integer DEFAULT_EMPTY_QUEUE_SLEEP_MILLIS = 500;
        private static final String DEFAULT_URI = "http://localhost:7090/metrics/v1/application";
        private static final Integer DEFAULT_PARALLELISM = 2;
    }
}
