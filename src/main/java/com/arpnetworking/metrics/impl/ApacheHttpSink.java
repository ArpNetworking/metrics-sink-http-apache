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
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.aggregation.protocol.Client;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
            final Client.Request.Builder requestBuilder = Client.Request.newBuilder();
            //TODO(brandon): actually serialize
            final Client.Request request = requestBuilder.build();
            return request.toByteArray();
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
