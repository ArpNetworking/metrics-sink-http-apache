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
import com.arpnetworking.metrics.Quantity;

/**
 * Interface for callbacks from client.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public interface ApacheHttpSinkEventHandler {

    /**
     * Callback invoked when a request to send samples has completed.
     *
     * @param records the number of records sent
     * @param bytes the number of bytes sent
     * @param success success or failure
     * @param elapasedTime the elapsed time
     */
    void attemptComplete(long records, long bytes, boolean success, Quantity elapasedTime);

    /**
     * Callback invoked when an {@link Event} is dropped from the queue.
     *
     * @param event the {@link Event} dropped from the queue
     */
    void droppedEvent(Event event);
}
