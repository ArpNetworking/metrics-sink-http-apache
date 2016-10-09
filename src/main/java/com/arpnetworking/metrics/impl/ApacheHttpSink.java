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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        _executor.execute(new HttpDispatch(event));
    }

    private ApacheHttpSink(final Builder builder) {
        //TODO(brandon): Limit the number of work units buffered
        _executor = Executors.newFixedThreadPool(builder._parallelism, (runnable) -> new Thread(runnable, "ApacheHttpSinkWorker"));
        final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setDefaultMaxPerRoute(builder._parallelism);
        connectionManager.setMaxTotal(builder._parallelism);
        _httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();
        _uri = builder._uri;
    }

    private final ExecutorService _executor;
    private final CloseableHttpClient _httpClient;
    private final String _uri;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ApacheHttpSink.class);

    private final class HttpDispatch implements Runnable {
        @Override
        public void run() {
            final HttpPost post = new HttpPost(_uri);

            post.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/octet-stream"));
            post.setEntity(new ByteArrayEntity(serializeEvent()));
            try {
                final CloseableHttpResponse response = _httpClient.execute(post);
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

        private byte[] serializeEvent() {
            final ClientV1.RecordSet.Builder requestBuilder = ClientV1.RecordSet.newBuilder();
            final ClientV1.Record.Builder builder = ClientV1.Record.newBuilder();
            for (Map.Entry<String, String> annotation : _event.getAnnotations().entrySet()) {
                builder.addAnnotations(
                        ClientV1.AnnotationEntry.newBuilder()
                                .setName(annotation.getKey())
                                .setValue(annotation.getValue())
                                .build());
            }

            for (Map.Entry<String, List<Quantity>> entry : _event.getCounterSamples().entrySet()) {
                builder.addCounters(ClientV1.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            for (Map.Entry<String, List<Quantity>> entry : _event.getTimerSamples().entrySet()) {
                builder.addTimers(ClientV1.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            for (Map.Entry<String, List<Quantity>> entry : _event.getGaugeSamples().entrySet()) {
                builder.addGauges(ClientV1.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            //TODO(brandonarp): just pull from dimensions once the library supports it
            final String host = _event.getAnnotations().get("_host");
            if (host != null) {
                builder.addDimensions(ClientV1.DimensionEntry.newBuilder().setName("host").setValue(host).build());
            }

            final String service = _event.getAnnotations().get("_service");
            if (service != null) {
                builder.addDimensions(ClientV1.DimensionEntry.newBuilder().setName("service").setValue(service).build());
            }

            final String cluster = _event.getAnnotations().get("_cluster");
            if (cluster != null) {
                builder.addDimensions(ClientV1.DimensionEntry.newBuilder().setName("cluster").setValue(cluster).build());
            }

            extractTimestamp("_end", builder::setEndMillisSinceEpoch);
            extractTimestamp("_start", builder::setStartMillisSinceEpoch);

            final String id = _event.getAnnotations().get("_id");
            if (id != null) {
                final UUID uuid = UUID.fromString(id);
                final ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
                buffer.putLong(uuid.getMostSignificantBits());
                buffer.putLong(uuid.getLeastSignificantBits());
                builder.setId(ByteString.copyFrom(buffer));
            }
            requestBuilder.addRecords(builder.build());
            final ClientV1.RecordSet request = requestBuilder.build();
            return request.toByteArray();
        }

        private void extractTimestamp(final String key, final LongConsumer setter) {
            final String endTimestamp = _event.getAnnotations().get(key);
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
            }

            return builder.build();
        }

        private HttpDispatch(final Event event) {
            _event = event;
        }

        private final Event _event;
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

        private static final Integer DEFAULT_BUFFER_SIZE = 10000;
        private static final String DEFAULT_URI = "http://localhost:7090/metrics/v1/application";
        private static final Integer DEFAULT_PARALLELISM = 5;
    }
}
