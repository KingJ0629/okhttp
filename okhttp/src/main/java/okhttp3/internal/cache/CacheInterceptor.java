/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package okhttp3.internal.cache;

import java.io.IOException;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.internal.Internal;
import okhttp3.internal.Util;
import okhttp3.internal.http.HttpCodec;
import okhttp3.internal.http.HttpHeaders;
import okhttp3.internal.http.HttpMethod;
import okhttp3.internal.http.RealResponseBody;
import okio.Buffer;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.Okio;
import okio.Sink;
import okio.Source;
import okio.Timeout;

import static java.net.HttpURLConnection.HTTP_NOT_MODIFIED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static okhttp3.internal.Util.closeQuietly;
import static okhttp3.internal.Util.discard;

/** Serves requests from the cache and writes responses to the cache. */
public final class CacheInterceptor implements Interceptor {
  final InternalCache cache;

  public CacheInterceptor(InternalCache cache) {
    this.cache = cache;
  }

  @Override public Response intercept(Chain chain) throws IOException {
    // 1.通过 cache 找到之前缓存的响应，但是该缓存如他的名字一样，仅仅是一个候选人。
    Response cacheCandidate = cache != null
        ? cache.get(chain.request())
        : null;
  
    // 2.获取当前的系统时间。
    long now = System.currentTimeMillis();
  
    // 3.通过 CacheStrategy 的工厂方法构造出 CacheStrategy 对象，并通过 get 方法返回。
    CacheStrategy strategy = new CacheStrategy.Factory(now, chain.request(), cacheCandidate).get();
    // 4.在 CacheStrategy 的构造过程中，会初始化 networkRequest 和 cacheResponse 这两个变量，分别表示要发起的网络请求和确定的缓存。
    Request networkRequest = strategy.networkRequest;
    Response cacheResponse = strategy.cacheResponse;

    if (cache != null) {
      cache.trackResponse(strategy);
    }
  
    // 5.如果曾经有候选的缓存，但是经过处理后 cacheResponse 不存在，那么关闭候选的缓存资源。
    if (cacheCandidate != null && cacheResponse == null) {
      closeQuietly(cacheCandidate.body()); // The cache candidate wasn't applicable. Close it.
    }
  
    // 6.如果要发起的请求为空，并且没有缓存，那么直接返回 504 给调用者。
    // If we're forbidden from using the network and the cache is insufficient, fail.
    if (networkRequest == null && cacheResponse == null) {
      return new Response.Builder()
          .request(chain.request())
          .protocol(Protocol.HTTP_1_1)
          .code(504)
          .message("Unsatisfiable Request (only-if-cached)")
          .body(Util.EMPTY_RESPONSE)
          .sentRequestAtMillis(-1L)
          .receivedResponseAtMillis(System.currentTimeMillis())
          .build();
    }
  
    // 7.如果不需要发起网络请求，那么直接将缓存返回给调用者。
    // If we don't need the network, we're done.
    if (networkRequest == null) {
      return cacheResponse.newBuilder()
          .cacheResponse(stripBody(cacheResponse))
          .build();
    }

    Response networkResponse = null;
    try {
      // 8.继续调用链的下一个步骤，按常理来说，走到这里就会真正地发起网络请求了。
      networkResponse = chain.proceed(networkRequest);
    } finally {
      // 9.保证在发生了异常的情况下，候选的缓存可以正常关闭。
      // If we're crashing on I/O or otherwise, don't leak the cache body.
      if (networkResponse == null && cacheCandidate != null) {
        closeQuietly(cacheCandidate.body());
      }
    }
  
    // 10.网络请求完成之后，假如之前有缓存，那么首先进行一些额外的处理。
    // If we have a cache response too, then we're doing a conditional get.
    if (cacheResponse != null) {
      // 10.1 假如是 304，那么根据缓存构造出返回的结果给调用者。
      /**
       * 304 Not Modified
       * 表示资源未被修改，因为请求头指定的版本If-Modified-Since或If-None-Match。
       * 在这种情况下，由于客户端仍然具有以前下载的副本，因此不需要重新传输资源。
       */
      if (networkResponse.code() == HTTP_NOT_MODIFIED) {
        Response response = cacheResponse.newBuilder()
             // 结合两者的头部字段。
            .headers(combine(cacheResponse.headers(), networkResponse.headers()))
             // 更新发送和接收请求的时间。
            .sentRequestAtMillis(networkResponse.sentRequestAtMillis())
            .receivedResponseAtMillis(networkResponse.receivedResponseAtMillis())
             // 更新缓存和请求的返回结果。
            .cacheResponse(stripBody(cacheResponse))
            .networkResponse(stripBody(networkResponse))
            .build();
        networkResponse.body().close();

        // Update the cache after combining headers but before stripping the
        // Content-Encoding header (as performed by initContentStream()).
        cache.trackConditionalCacheHit();
        cache.update(cacheResponse, response);
        return response;
      } else {
        // 10.2 关闭缓存。
        closeQuietly(cacheResponse.body());
      }
    }
    
    // 11.构造出返回结果。
    Response response = networkResponse.newBuilder()
        .cacheResponse(stripBody(cacheResponse))
        .networkResponse(stripBody(networkResponse))
        .build();

    if (cache != null) {
      if (HttpHeaders.hasBody(response) && CacheStrategy.isCacheable(response, networkRequest)) {
        // 12.如果符合缓存的要求，那么就缓存该结果。
        // Offer this request to the cache.
        CacheRequest cacheRequest = cache.put(response);
        return cacheWritingResponse(cacheRequest, response);
      }
  
      // 13.对于某些请求方法，需要移除缓存，例如 PUT/PATCH/POST/DELETE/MOVE
      if (HttpMethod.invalidatesCache(networkRequest.method())) {
        try {
          cache.remove(networkRequest);
        } catch (IOException ignored) {
          // The cache cannot be written.
        }
      }
    }

    return response;
  }

  private static Response stripBody(Response response) {
    return response != null && response.body() != null
        ? response.newBuilder().body(null).build()
        : response;
  }

  /**
   * Returns a new source that writes bytes to {@code cacheRequest} as they are read by the source
   * consumer. This is careful to discard bytes left over when the stream is closed; otherwise we
   * may never exhaust the source stream and therefore not complete the cached response.
   */
  private Response cacheWritingResponse(final CacheRequest cacheRequest, Response response)
      throws IOException {
    // Some apps return a null body; for compatibility we treat that like a null cache request.
    if (cacheRequest == null) return response;
    Sink cacheBodyUnbuffered = cacheRequest.body();
    if (cacheBodyUnbuffered == null) return response;

    final BufferedSource source = response.body().source();
    final BufferedSink cacheBody = Okio.buffer(cacheBodyUnbuffered);

    Source cacheWritingSource = new Source() {
      boolean cacheRequestClosed;

      @Override public long read(Buffer sink, long byteCount) throws IOException {
        long bytesRead;
        try {
          bytesRead = source.read(sink, byteCount);
        } catch (IOException e) {
          if (!cacheRequestClosed) {
            cacheRequestClosed = true;
            cacheRequest.abort(); // Failed to write a complete cache response.
          }
          throw e;
        }

        if (bytesRead == -1) {
          if (!cacheRequestClosed) {
            cacheRequestClosed = true;
            cacheBody.close(); // The cache response is complete!
          }
          return -1;
        }

        sink.copyTo(cacheBody.buffer(), sink.size() - bytesRead, bytesRead);
        cacheBody.emitCompleteSegments();
        return bytesRead;
      }

      @Override public Timeout timeout() {
        return source.timeout();
      }

      @Override public void close() throws IOException {
        if (!cacheRequestClosed
            && !discard(this, HttpCodec.DISCARD_STREAM_TIMEOUT_MILLIS, MILLISECONDS)) {
          cacheRequestClosed = true;
          cacheRequest.abort();
        }
        source.close();
      }
    };

    String contentType = response.header("Content-Type");
    long contentLength = response.body().contentLength();
    return response.newBuilder()
        .body(new RealResponseBody(contentType, contentLength, Okio.buffer(cacheWritingSource)))
        .build();
  }

  /** Combines cached headers with a network headers as defined by RFC 7234, 4.3.4. */
  private static Headers combine(Headers cachedHeaders, Headers networkHeaders) {
    Headers.Builder result = new Headers.Builder();

    for (int i = 0, size = cachedHeaders.size(); i < size; i++) {
      String fieldName = cachedHeaders.name(i);
      String value = cachedHeaders.value(i);
      if ("Warning".equalsIgnoreCase(fieldName) && value.startsWith("1")) {
        continue; // Drop 100-level freshness warnings.
      }
      if (isContentSpecificHeader(fieldName) || !isEndToEnd(fieldName)
              || networkHeaders.get(fieldName) == null) {
        Internal.instance.addLenient(result, fieldName, value);
      }
    }

    for (int i = 0, size = networkHeaders.size(); i < size; i++) {
      String fieldName = networkHeaders.name(i);
      if (!isContentSpecificHeader(fieldName) && isEndToEnd(fieldName)) {
        Internal.instance.addLenient(result, fieldName, networkHeaders.value(i));
      }
    }

    return result.build();
  }

  /**
   * Returns true if {@code fieldName} is an end-to-end HTTP header, as defined by RFC 2616,
   * 13.5.1.
   */
  static boolean isEndToEnd(String fieldName) {
    return !"Connection".equalsIgnoreCase(fieldName)
        && !"Keep-Alive".equalsIgnoreCase(fieldName)
        && !"Proxy-Authenticate".equalsIgnoreCase(fieldName)
        && !"Proxy-Authorization".equalsIgnoreCase(fieldName)
        && !"TE".equalsIgnoreCase(fieldName)
        && !"Trailers".equalsIgnoreCase(fieldName)
        && !"Transfer-Encoding".equalsIgnoreCase(fieldName)
        && !"Upgrade".equalsIgnoreCase(fieldName);
  }

  /**
   * Returns true if {@code fieldName} is content specific and therefore should always be used
   * from cached headers.
   */
  static boolean isContentSpecificHeader(String fieldName) {
    return "Content-Length".equalsIgnoreCase(fieldName)
        || "Content-Encoding".equalsIgnoreCase(fieldName)
        || "Content-Type".equalsIgnoreCase(fieldName);
  }
}
