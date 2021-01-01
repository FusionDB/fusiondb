/*
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
package com.facebook.presto.metadata;

import com.facebook.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpClient.HttpResponseFuture;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class HttpRemoteNodeAsyncCoordinator
{
    private static final Logger log = Logger.get(HttpRemoteNodeAsyncCoordinator.class);

    private final HttpClient httpClient;
    private final URI coordinatorInfoUri;
    private final AtomicReference<Optional<Boolean>> asyncCoordinatorState = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final AtomicLong lastWarningLogged = new AtomicLong();
    private final Set<InternalNode> coordinatorNodes;

    public HttpRemoteNodeAsyncCoordinator(HttpClient httpClient, URI coordinatorInfoUri, Set<InternalNode> coordinatorNodes)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.coordinatorInfoUri = requireNonNull(coordinatorInfoUri, "coordinatorInfoUri is null");
        this.coordinatorNodes = requireNonNull(coordinatorNodes, "coordinatorNodes is null");
    }

    public Optional<Boolean> getAsyncCoordinatorState()
    {
        return asyncCoordinatorState.get();
    }

    public synchronized void asyncRefresh()
    {
        Duration sinceUpdate = nanosSince(lastUpdateNanos.get());
        if (nanosSince(lastWarningLogged.get()).toMillis() > 1_000 &&
                sinceUpdate.toMillis() > 10_000 &&
                future.get() != null) {
            log.warn("Node state update request to %s has not returned in %s", coordinatorInfoUri, sinceUpdate.toString(SECONDS));
            lastWarningLogged.set(System.nanoTime());
        }
        if (sinceUpdate.toMillis() > 1_000 && future.get() == null) {
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new ParameterNamesModule())
                    .registerModule(new Jdk8Module())
                    .registerModule(new JavaTimeModule()); // new module, NOT JSR310Module
            String coordinatorNodesString = null;
            try {
                coordinatorNodesString = mapper.writeValueAsString(coordinatorNodes);
            }
            catch (JsonProcessingException e) {
                log.info(e.getMessage(), e);
            }
            Request request = setContentTypeHeaders(false, preparePost())
                    .setUri(coordinatorInfoUri)
                    .setBodyGenerator(createStaticBodyGenerator(coordinatorNodesString, UTF_8))
                    .build();
            HttpResponseFuture<JsonResponse<Boolean>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(jsonCodec(Boolean.class)));
            future.compareAndSet(null, responseFuture);

            Futures.addCallback(responseFuture, new FutureCallback<JsonResponse<Boolean>>()
            {
                @Override
                public void onSuccess(@Nullable JsonResponse<Boolean> result)
                {
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                    if (result != null) {
                        if (result.hasValue()) {
                            asyncCoordinatorState.set(Optional.ofNullable(result.getValue()));
                        }
                        if (result.getStatusCode() != OK.code()) {
                            log.warn("Error async coordinator info to worker node from %s returned status %d: %s", coordinatorInfoUri, result.getStatusCode(), result.getStatusMessage());
                            return;
                        }
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    log.warn("Error async coordinator info to worker node from %s: %s", coordinatorInfoUri, t.getMessage());
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                }
            }, directExecutor());
        }
    }
}
