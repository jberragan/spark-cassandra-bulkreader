package org.apache.cassandra.spark.utils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.spark.utils.streaming.Source;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import static org.apache.cassandra.spark.utils.SSTableInputStreamTests.DEFAULT_CHUNK_SIZE;
import static org.apache.cassandra.spark.utils.SSTableInputStreamTests.STATS;
import static org.apache.cassandra.spark.utils.SSTableInputStreamTests.randBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

/**
 * Test the {@link BufferingInputStream} by firing up an in-test http server and reading the files with an http client
 * Compares the MD5s to verify file bytes match bytes returned by {@link BufferingInputStream}.
 */
public class SSTableInputStreamHttpTests
{
    static final ExecutorService HTTP_EXECUTOR = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("http-server-%d").setDaemon(true).build());
    static final ExecutorService HTTP_CLIENT = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("http-client-%d").setDaemon(true).build());
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableInputStreamHttpTests.class);

    @ClassRule
    public static TemporaryFolder DIR = new TemporaryFolder();
    private static final String HOST = "localhost";
    private static final int PORT = 8001;
    private static final int HTTP_CLIENT_CHUNK_SIZE = 8192;

    private static HttpServer SERVER;
    private static CloseableHttpClient CLIENT;

    @BeforeClass
    public static void setup() throws IOException
    {
        // create in-test http server to handle range requests and tmp files
        SERVER = HttpServer.create(new InetSocketAddress(HOST, PORT), 0);
        SERVER.setExecutor(HTTP_EXECUTOR);
        SERVER.createContext("/", exchange -> {
            try
            {
                final String uri = exchange.getRequestURI().getPath();
                final Path path = Paths.get(DIR.getRoot().getPath(), uri);

                // extract Range from header
                final long size = Files.size(path);
                final long start, end;
                final String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                if (rangeHeader != null)
                {
                    final String[] range = rangeHeader.split("=")[1].split("-");
                    start = Long.parseLong(range[0]);
                    final long endValue = Long.parseLong(range[1]);
                    if (endValue < start)
                    {
                        exchange.sendResponseHeaders(416, -1);
                        return;
                    }
                    end = Math.min(size - 1, endValue);
                }
                else
                {
                    start = 0;
                    end = size;
                }

                // return file bytes within range
                final int len = (int) (end - start + 1);
                if (len <= 0)
                {
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }

                LOGGER.info("Serving file filename={} start={} end={} len={}", path.getFileName(), start, end, len);
                exchange.sendResponseHeaders(200, len);
                try (final RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r"))
                {
                    final byte[] b = new byte[len];
                    raf.seek(start);
                    raf.read(b, 0, len);
                    exchange.getResponseBody().write(b);
                }
                exchange.getResponseBody().flush();
            }
            catch (final Throwable t)
            {
                LOGGER.error("Unexpected exception in in-test http server", t);
                exchange.sendResponseHeaders(500, -1);
            }
            finally
            {
                exchange.close();
            }
        });
        SERVER.start();
        LOGGER.info("Started in-test HTTP Server host={} port={}", HOST, PORT);

        CLIENT = HttpClients.createDefault();
    }

    @AfterClass
    public static void tearDown()
    {
        SERVER.stop(0);
    }

    // http client source for reading SSTable from http server
    private static Source<SSTable> buildHttpSource(String filename, long size, Long maxBufferSize, Long chunkBufferSize)
    {
        return new Source<SSTable>()
        {
            public void request(long start, long end, StreamConsumer consumer)
            {
                CompletableFuture.runAsync(() -> {
                    LOGGER.info("Reading file using http client filename={} start={} end={}", filename, start, end);
                    final HttpGet get = new HttpGet("http://" + HOST + ":" + PORT + "/" + filename);
                    get.setHeader("Range", String.format("bytes=%d-%d", start, end));
                    try
                    {
                        final CloseableHttpResponse resp = CLIENT.execute(get);
                        if (resp.getStatusLine().getStatusCode() == 200)
                        {
                            try (final InputStream is = resp.getEntity().getContent())
                            {
                                int len;
                                byte[] ar = new byte[HTTP_CLIENT_CHUNK_SIZE];
                                while ((len = is.read(ar, 0, ar.length)) != -1)
                                {
                                    if (len < HTTP_CLIENT_CHUNK_SIZE)
                                    {
                                        final byte[] copy = new byte[len];
                                        System.arraycopy(ar, 0, copy, 0, len);
                                        ar = copy;
                                    }
                                    consumer.onRead(StreamBuffer.wrap(ar));
                                    ar = new byte[HTTP_CLIENT_CHUNK_SIZE];
                                }
                                consumer.onEnd();
                            }
                        }
                        else
                        {
                            consumer.onError(new RuntimeException("Unexpected status code: " + resp.getStatusLine().getStatusCode()));
                        }
                    }
                    catch (IOException e)
                    {
                        consumer.onError(e);
                    }
                }, HTTP_CLIENT);
            }

            public long maxBufferSize()
            {
                return maxBufferSize != null ? maxBufferSize : Source.super.maxBufferSize();
            }

            public long chunkBufferSize()
            {
                return chunkBufferSize != null ? chunkBufferSize : Source.super.chunkBufferSize();
            }

            public SSTable file()
            {
                return null;
            }

            public CassandraFile.FileType fileType()
            {
                return null;
            }

            public long size()
            {
                return size;
            }
        };
    }

    // tests

    @Test
    public void testSmallChunkSizes()
    {
        final List<Long> fileSizes = Arrays.asList(1L, 1536L, 32768L, 250000L, 10485800L);
        qt().forAll(arbitrary().pick(fileSizes)).checkAssert((size) -> runHttpTest(size, 8192L, 4096L));
    }

    @Test
    public void testDefaultClientConfig()
    {
        final List<Long> fileSizes = Arrays.asList(1L, 13L, 512L, 1024L, 1536L, 32768L, 1000000L, 10485800L, 1000000000L);
        qt().forAll(arbitrary().pick(fileSizes)).checkAssert(this::runHttpTest);
    }

    // http tester

    private void runHttpTest(final long size)
    {
        runHttpTest(size, null, null);
    }

    // test SSTableInputStream against test http server using http client
    private void runHttpTest(final long size, Long maxBufferSize, Long chunkBufferSize)
    {
        try
        {
            // create tmp file with random data
            final Path path = DIR.newFile().toPath();
            final MessageDigest digest = DigestUtils.getMd5Digest();
            try (final BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(path)))
            {
                long remaining = size;
                while (remaining > 0)
                {
                    final byte[] b = randBytes((int) Math.min(remaining, DEFAULT_CHUNK_SIZE));
                    out.write(b);
                    digest.update(b);
                    remaining -= b.length;
                }
            }
            final byte[] expectedMD5 = digest.digest();
            assertEquals(size, Files.size(path));
            LOGGER.info("Created random file path={} fileSize={}", path, size);

            // use http client source to read files across http
            // and verify MD5 matches expected
            final byte[] actualMD5;
            long blockingTimeMillis;
            final Source<SSTable> source = buildHttpSource(path.getFileName().toString(), Files.size(path), maxBufferSize, chunkBufferSize);
            try (final BufferingInputStream<SSTable> is = new BufferingInputStream<>(source, STATS))
            {
                actualMD5 = DigestUtils.md5(is);
                blockingTimeMillis = TimeUnit.MILLISECONDS.convert(is.timeBlockedNanos(), TimeUnit.NANOSECONDS);
                assertEquals(size, is.bytesRead());
                assertEquals(0L, is.bytesBuffered());
            }
            LOGGER.info("Time spent blocking on InputStream thread blockingTimeMillis={} fileSize={}", blockingTimeMillis, size);
            assertArrayEquals(actualMD5, expectedMD5);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
