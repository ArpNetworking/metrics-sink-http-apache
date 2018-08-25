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

import com.arpnetworking.commons.java.util.function.SingletonSupplier;
import com.arpnetworking.commons.slf4j.RateLimitedLogger;
import com.arpnetworking.metrics.CompoundUnit;
import com.arpnetworking.metrics.Event;
import com.arpnetworking.metrics.Quantity;
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.StopWatch;
import com.arpnetworking.metrics.Unit;
import com.google.protobuf.ByteString;
import com.inscopemetrics.client.protocol.ClientV2;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Http sink using the Protobuf format for metrics and the Apache HTTP library.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class ApacheHttpSink implements Sink {

    @Override
    public void record(final Event event) {
        _events.push(event);
        final int events = _eventsCount.incrementAndGet();
        if (events > _bufferSize) {
            _eventsDroppedLogger.getLogger().warn(
                    "Event queue is full; dropping event(s)");
            final Event droppedEvent = _events.pollFirst();
            _eventHandler.ifPresent(eh -> eh.droppedEvent(droppedEvent));
            _eventsCount.decrementAndGet();
        }
    }

    /* package private */ void stop() {
        _isRunning.set(false);
    }

    /* package private */ static void safeSleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            // Do nothing
        }
    }

    private ApacheHttpSink(final Builder builder) {
        this(builder, LOGGER);
    }

    /* package private */ ApacheHttpSink(final Builder builder, final Logger logger) {
        this(
                builder,
                new SingletonSupplier<>(() -> {
                    final SingletonSupplier<PoolingHttpClientConnectionManager> clientManagerSupplier = new SingletonSupplier<>(() -> {
                        final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
                        connectionManager.setDefaultMaxPerRoute(builder._parallelism);
                        connectionManager.setMaxTotal(builder._parallelism);
                        return connectionManager;
                    });

                    return HttpClients.custom()
                            .setConnectionManager(clientManagerSupplier.get())
                            .build();
                }),
                logger);
    }

    /* package private */ ApacheHttpSink(
            final Builder builder,
            final Supplier<CloseableHttpClient> httpClientSupplier,
            final org.slf4j.Logger logger) {
        _uri = builder._uri;
        _bufferSize = builder._bufferSize;
        _maxBatchSize = builder._maxBatchSize;
        _emptyQueueInterval = builder._emptyQueueInterval;
        _eventHandler = Optional.ofNullable(builder._eventHandler);
        _eventsDroppedLogger = new RateLimitedLogger(
                "EventsDroppedLogger",
                logger,
                builder._eventsDroppedLoggingInterval);

        final ExecutorService executor = Executors.newFixedThreadPool(
                builder._parallelism,
                runnable -> new Thread(runnable, "MetricsSinkApacheHttpWorker"));

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(this._isRunning, executor));

        // Use a single shared worker instance across the pool; see getSharedHttpClient
        final HttpDispatch httpDispatchWorker = new HttpDispatch(
                httpClientSupplier,
                builder._eventsDroppedLoggingInterval,
                logger,
                _eventHandler);
        for (int i = 0; i < builder._parallelism; ++i) {
            executor.execute(httpDispatchWorker);
        }
    }

    private final URI _uri;
    private final int _bufferSize;
    private final int _maxBatchSize;
    private final Duration _emptyQueueInterval;
    private final Optional<ApacheHttpSinkEventHandler> _eventHandler;
    private final RateLimitedLogger _eventsDroppedLogger;
    private final Deque<Event> _events = new ConcurrentLinkedDeque<>();
    private final AtomicInteger _eventsCount = new AtomicInteger(0);
    private final AtomicBoolean _isRunning = new AtomicBoolean(true);

    private static final Header CONTENT_TYPE_HEADER = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ApacheHttpSink.class);

    private final class HttpDispatch implements Runnable {

        /* package private */ HttpDispatch(
                final Supplier<CloseableHttpClient> httpClientSupplier,
                final Duration dispatchErrorLoggingInterval,
                final org.slf4j.Logger logger,
                final Optional<ApacheHttpSinkEventHandler> eventHandler) {
            _httpClientSupplier = httpClientSupplier;
            _dispatchErrorLogger = new RateLimitedLogger(
                    "DispatchErrorLogger",
                    logger,
                    dispatchErrorLoggingInterval);
            _eventHandler = eventHandler;
        }

        @Override
        public void run() {
            while (_isRunning.get()) {
                try {
                    // Collect a set of events to send
                    // NOTE: This also builds the serialization types in order to spend more time
                    // allowing more records to arrive in the batch
                    int collected = 0;
                    final ClientV2.RecordSet.Builder requestBuilder = ClientV2.RecordSet.newBuilder();
                    do {
                        final @Nullable Event event = _events.pollFirst();
                        if (event == null) {
                            break;
                        }

                        _eventsCount.decrementAndGet();
                        collected++;

                        requestBuilder.addRecords(serializeEvent(event));
                    } while (collected < _maxBatchSize);

                    if (collected > 0) {
                        dispatchRequest(_httpClientSupplier.get(), requestBuilder.build());
                    }

                    // Sleep if the queue is empty
                    if (collected == 0) {
                        safeSleep(_emptyQueueInterval.toMillis());
                    }
                    // CHECKSTYLE.OFF: IllegalCatch - Ensure exception neutrality
                } catch (final RuntimeException e) {
                    // CHECKSTYLE.ON: IllegalCatch
                    _dispatchErrorLogger.getLogger().error("MetricsSinkApacheHttpWorker failure", e);
                }
            }
        }

        private void dispatchRequest(final CloseableHttpClient httpClient, final ClientV2.RecordSet recordSet) {
            // TODO(ville): We need to add retries.
            // Requests that fail should either go into a different retry queue
            // or else be requeued at the front of the existing queue (unless
            // it's full). The data will need to be wrapped with an attempt
            // count.
            final ByteArrayEntity entity = new ByteArrayEntity(recordSet.toByteArray());
            final HttpPost post = new HttpPost(_uri);
            post.setHeader(CONTENT_TYPE_HEADER);
            post.setEntity(entity);

            final StopWatch stopWatch = StopWatch.start();
            boolean success = false;

            try (CloseableHttpResponse response = httpClient.execute(post)) {
                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == HttpStatus.SC_NOT_FOUND) {
                    // While the server may be running and accepting HTTP
                    // requests, the specific endpoint may not have been loaded
                    // yet. Therefore, this case should not be treated any
                    // differently than the server not being available. However,
                    // it could also mean that the server does not support the
                    // particular endpoint in use (e.g. version mismatch).
                    throw new RuntimeException("Endpoint not available");
                }
                if ((statusCode / 100) != 2) {
                    _dispatchErrorLogger.getLogger().error(
                            String.format(
                                    "Received failure response when sending metrics to HTTP endpoint; uri=%s, status=%s",
                                    _uri,
                                    statusCode));
                } else {
                    success = true;
                }
                // CHECKSTYLE.OFF: IllegalCatch - Prevent leaking exceptions; it makes testing more complex
            } catch (final RuntimeException | IOException e) {
                // CHECKSTYLE.ON: IllegalCatch
                _dispatchErrorLogger.getLogger().error(
                        String.format(
                                "Encountered failure when sending metrics to HTTP endpoint; uri=%s",
                                _uri),
                        e);
            } finally {
                stopWatch.stop();
                if (_eventHandler.isPresent()) {
                    _eventHandler.get().attemptComplete(
                            recordSet.getRecordsCount(),
                            entity.getContentLength(),
                            success,
                            stopWatch.getElapsedTime());
                }
            }
        }

        private ClientV2.Record serializeEvent(final Event event) {
            final ClientV2.Record.Builder builder = ClientV2.Record.newBuilder();
            for (final Map.Entry<String, String> annotation : event.getAnnotations().entrySet()) {
                builder.addAnnotations(
                        ClientV2.AnnotationEntry.newBuilder()
                                .setName(annotation.getKey())
                                .setValue(annotation.getValue())
                                .build());
            }

            for (final Map.Entry<String, List<Quantity>> entry : event.getCounterSamples().entrySet()) {
                builder.addCounters(ClientV2.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            for (final Map.Entry<String, List<Quantity>> entry : event.getTimerSamples().entrySet()) {
                builder.addTimers(ClientV2.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            for (final Map.Entry<String, List<Quantity>> entry : event.getGaugeSamples().entrySet()) {
                builder.addGauges(ClientV2.MetricEntry.newBuilder()
                        .addAllSamples(buildQuantities(entry.getValue()))
                        .setName(entry.getKey())
                        .build());
            }

            //TODO(brandonarp): just pull from dimensions once the library supports it
            final String host = event.getAnnotations().get("_host");
            if (host != null) {
                builder.addDimensions(ClientV2.DimensionEntry.newBuilder().setName("host").setValue(host).build());
            }

            final String service = event.getAnnotations().get("_service");
            if (service != null) {
                builder.addDimensions(ClientV2.DimensionEntry.newBuilder().setName("service").setValue(service).build());
            }

            final String cluster = event.getAnnotations().get("_cluster");
            if (cluster != null) {
                builder.addDimensions(ClientV2.DimensionEntry.newBuilder().setName("cluster").setValue(cluster).build());
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

        private List<ClientV2.Quantity> buildQuantities(final List<Quantity> quantities) {
            final List<ClientV2.Quantity> timerQuantities = new ArrayList<>(quantities.size());
            for (final Quantity quantity : quantities) {
                final ClientV2.Quantity.Builder quantityBuilder = ClientV2.Quantity.newBuilder();
                final Number quantityValue = quantity.getValue();
                if (quantityValue instanceof Long
                        || quantityValue instanceof Integer
                        || quantityValue instanceof Short
                        || quantityValue instanceof Byte) {
                    quantityBuilder.setLongValue(quantityValue.longValue());
                } else {
                    quantityBuilder.setDoubleValue(quantityValue.doubleValue());
                }
                final ClientV2.CompoundUnit unit = buildUnit(quantity.getUnit());
                quantityBuilder.setUnit(unit);
                timerQuantities.add(quantityBuilder.build());
            }
            return timerQuantities;
        }

        private ClientV2.CompoundUnit buildUnit(@Nullable final Unit unit) {
            final ClientV2.CompoundUnit.Builder builder = ClientV2.CompoundUnit.newBuilder();

            if (unit instanceof CompoundUnit) {
                final CompoundUnit compoundUnit = (CompoundUnit) unit;
                for (final Unit numeratorUnit : compoundUnit.getNumeratorUnits()) {
                    builder.addNumerator(mapUnit(numeratorUnit));
                }

                for (final Unit denominatorUnit : compoundUnit.getDenominatorUnits()) {
                    builder.addDenominator(mapUnit(denominatorUnit));
                }
            } else if (unit != null) {
                final ClientV2.Unit numeratorUnit = mapUnit(unit);
                if (numeratorUnit != null) {
                    builder.addNumerator(numeratorUnit);
                }
            }
            return builder.build();
        }

        private @Nullable ClientV2.Unit mapUnit(final Unit unit) {
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

            final ClientV2.Unit.Builder builder = ClientV2.Unit.newBuilder();
            try {
                builder.setType(ClientV2.Unit.Type.Value.valueOf(baseUnit.name()));
            } catch (final IllegalArgumentException e) {
                _dispatchErrorLogger.getLogger().warn(
                        String.format("Dropping unsupported unit %s", baseUnit.name()));
                builder.setType(ClientV2.Unit.Type.Value.UNKNOWN);
            }
            if (baseScale != null) {
                try {
                    builder.setScale(ClientV2.Unit.Scale.Value.valueOf(baseScale.name()));
                } catch (final IllegalArgumentException e) {
                    _dispatchErrorLogger.getLogger().warn(
                            String.format("Dropping unsupported scale %s", baseScale.name()));
                    builder.setScale(ClientV2.Unit.Scale.Value.UNKNOWN);
                }
            } else {
                builder.setScale(ClientV2.Unit.Scale.Value.UNIT);
            }

            return builder.build();
        }

        private final Supplier<CloseableHttpClient> _httpClientSupplier;
        private final RateLimitedLogger _dispatchErrorLogger;
        private final Optional<ApacheHttpSinkEventHandler> _eventHandler;
    }

    private static final class ShutdownHookThread extends Thread {

        /* package private */ ShutdownHookThread(
                final AtomicBoolean isRunning,
                final ExecutorService executor) {
            this._isRunning = isRunning;
            _executor = executor;
        }

        @Override
        public void run() {
            _isRunning.set(false);
            _executor.shutdown();
        }

        private final AtomicBoolean _isRunning;
        private final ExecutorService _executor;
    }

    /**
     * Builder for {@link ApacheHttpSink}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder {

        /**
         * Set the buffer size in number of events. Optional; default is 10,000.
         *
         * @param value The number of events to buffer.
         * @return This {@link Builder} instance.
         */
        public Builder setBufferSize(@Nullable final Integer value) {
            _bufferSize = value;
            return this;
        }

        /**
         * Set the URI of the HTTP endpoint. Optional; default is
         * {@code http://localhost:7090/metrics/v1/application}.
         *
         * @param value The uri of the HTTP endpoint.
         * @return This {@link Builder} instance.
         */
        public Builder setUri(@Nullable final URI value) {
            _uri = value;
            return this;
        }

        /**
         * Set the parallelism (threads and connections) that the sink will
         * use. Optional; default is 5.
         *
         * @param value The uri of the HTTP endpoint.
         * @return This {@link Builder} instance.
         */
        public Builder setParallelism(@Nullable final Integer value) {
            _parallelism = value;
            return this;
        }

        /**
         * Set the empty queue interval. Each worker thread will independently
         * sleep this long if the event queue is empty. Optional; default is
         * 500 milliseconds.
         *
         * @param value The empty queue interval.
         * @return This {@link Builder} instance.
         */
        public Builder setEmptyQueueInterval(@Nullable final Duration value) {
            _emptyQueueInterval = value;
            return this;
        }

        /**
         * Set the maximum batch size (records to send in each request).
         * Optional; default is 500.
         *
         * @param value The maximum batch size.
         * @return This {@link Builder} instance.
         */
        public Builder setMaxBatchSize(@Nullable final Integer value) {
            _maxBatchSize = value;
            return this;
        }

        /**
         * Set the events dropped logging interval. Any event drop notices will
         * be logged at most once per interval. Optional; default is 1 minute.
         *
         * @param value The logging interval.
         * @return This {@link Builder} instance.
         */
        public Builder setEventsDroppedLoggingInteval(@Nullable final Duration value) {
            _eventsDroppedLoggingInterval = value;
            return this;
        }

        /**
         * Set the dispatch error logging interval. Any dispatch errors will be
         * logged at most once per interval. Optional; default is 1 minute.
         *
         * @param value The logging interval.
         * @return This {@link Builder} instance.
         */
        public Builder setDispatchErrorLoggingInterval(@Nullable final Duration value) {
            _dispatchErrorLoggingInterval = value;
            return this;
        }

        /**
         * Set the event handler. Optional; default is {@code null}.
         *
         * @param value The event handler.
         * @return This {@link Builder} instance.
         */
        public Builder setEventHandler(@Nullable final ApacheHttpSinkEventHandler value) {
            _eventHandler = value;
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
                        "Unable to construct %s, sink disabled; failures=%s",
                        this.getClass().getEnclosingClass().getSimpleName(),
                        failures));
                return new WarningSink.Builder()
                        .setReasons(failures)
                        .build();
            }

            return new ApacheHttpSink(this);
        }

        private void applyDefaults() {
            if (_bufferSize == null) {
                _bufferSize = DEFAULT_BUFFER_SIZE;
                LOGGER.info(String.format(
                        "Defaulted null buffer size; bufferSize=%s",
                        _bufferSize));
            }
            if (_uri == null) {
                _uri = DEFAULT_URI;
                LOGGER.info(String.format(
                        "Defaulted null uri; uri=%s",
                        _uri));
            }
            if (_parallelism == null) {
                _parallelism = DEFAULT_PARALLELISM;
                LOGGER.info(String.format(
                        "Defaulted null parallelism; parallelism=%s",
                        _parallelism));
            }
            if (_maxBatchSize == null) {
                _maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
                LOGGER.info(String.format(
                        "Defaulted null max batch size; maxBatchSize=%s",
                        _maxBatchSize));
            }
            if (_emptyQueueInterval == null) {
                _emptyQueueInterval = DEFAULT_EMPTY_QUEUE_INTERVAL;
                LOGGER.info(String.format(
                        "Defaulted null empty queue interval; emptyQueueInterval=%s",
                        _emptyQueueInterval));
            }
            if (_eventsDroppedLoggingInterval == null) {
                _eventsDroppedLoggingInterval = DEFAULT_EVENTS_DROPPED_LOGGING_INTERVAL;
                LOGGER.info(String.format(
                        "Defaulted null dispatch error logging interval; eventsDroppedLoggingInterval=%s",
                        _eventsDroppedLoggingInterval));
            }
            if (_dispatchErrorLoggingInterval == null) {
                _dispatchErrorLoggingInterval = DEFAULT_DISPATCH_ERROR_LOGGING_INTERVAL;
                LOGGER.info(String.format(
                        "Defaulted null dispatch error logging interval; dispatchErrorLoggingInterval=%s",
                        _dispatchErrorLoggingInterval));
            }
        }

        private void validate(final List<String> failures) {
            if (_uri.getScheme() == null
                    || (!_uri.getScheme().equalsIgnoreCase("http") && !_uri.getScheme().equalsIgnoreCase("https"))) {
                failures.add(String.format("URI must be an http(s) URI; uri=%s", _uri));
            }
        }

        private Integer _bufferSize = DEFAULT_BUFFER_SIZE;
        private URI _uri = DEFAULT_URI;
        private Integer _parallelism = DEFAULT_PARALLELISM;
        private Integer _maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
        private Duration _emptyQueueInterval = DEFAULT_EMPTY_QUEUE_INTERVAL;
        private Duration _eventsDroppedLoggingInterval = DEFAULT_EVENTS_DROPPED_LOGGING_INTERVAL;
        private Duration _dispatchErrorLoggingInterval = DEFAULT_DISPATCH_ERROR_LOGGING_INTERVAL;
        private @Nullable ApacheHttpSinkEventHandler _eventHandler;

        private static final Integer DEFAULT_BUFFER_SIZE = 10000;
        private static final URI DEFAULT_URI = URI.create("http://localhost:7090/metrics/v2/application");
        private static final Integer DEFAULT_PARALLELISM = 2;
        private static final Integer DEFAULT_MAX_BATCH_SIZE = 500;
        private static final Duration DEFAULT_EMPTY_QUEUE_INTERVAL = Duration.ofMillis(500);
        private static final Duration DEFAULT_EVENTS_DROPPED_LOGGING_INTERVAL = Duration.ofMinutes(1);
        private static final Duration DEFAULT_DISPATCH_ERROR_LOGGING_INTERVAL = Duration.ofMinutes(1);
    }
}
