package org.apache.cassandra.spark.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

public class FutureUtils
{
    private FutureUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Await all futures and combine into single result.
     *
     * @param futures             list of futures
     * @param acceptPartialResult if false, fail the entire request if a single failure occurs, if true just log partial failures
     * @param logger              consumer to log errors
     * @param <T>                 result type returned by this method
     * @return result of all combined futures
     */
    public static <T> List<T> awaitAll(final List<CompletableFuture<T>> futures,
                                       final boolean acceptPartialResult,
                                       final Consumer<Throwable> logger)
    {
        final List<T> result = new ArrayList<>(futures.size() * 10);
        for (CompletableFuture<T> future : futures)
        {
            final FutureResult<T> fr = await(future, logger);
            if (fr.throwable != null)
            {
                // failed
                if (!acceptPartialResult)
                {
                    throw new RuntimeException(ThrowableUtils.rootCause(fr.throwable));
                }
            }
            else if (fr.value != null)
            {
                // success
                result.add(fr.value);
            }
        }

        return result;
    }

    public static class FutureResult<T>
    {
        @Nullable
        public final T value;
        @Nullable
        public final Throwable throwable;

        private FutureResult(@Nullable T value,
                             @Nullable Throwable throwable)
        {
            this.value = value;
            this.throwable = throwable;
        }

        public static <T> FutureResult<T> failed(@NotNull final Throwable t)
        {
            return new FutureResult<>(null, t);
        }

        public static <T> FutureResult<T> success(@Nullable final T value)
        {
            return new FutureResult<>(value, null);
        }

        @Nullable
        public T value()
        {
            return value;
        }

        public boolean isSuccess()
        {
            return throwable == null;
        }
    }

    /**
     * Await a future and return result.
     *
     * @param future the future
     * @param logger consumer to log errors
     * @param <T>    result type returned by this method
     * @return result of the future
     */
    @NotNull
    public static <T> FutureResult<T> await(final CompletableFuture<T> future,
                                            final Consumer<Throwable> logger)
    {
        try
        {
            return FutureResult.success(future.get());
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            logger.accept(e);
            return FutureResult.failed(e);
        }
    }

    /**
     * Combine futures into a single future that completes when all futures complete successfully, or fails if any future fails.
     *
     * @param futures array of futures
     * @param <T>     result type returned by this method
     * @return a single future that combines all future results.
     */
    public static <T> CompletableFuture<List<T>> combine(List<CompletableFuture<T>> futures)
    {
        final CompletableFuture<List<T>> result = new CompletableFuture<>();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                         .whenComplete((aVoid, throwable) -> {
                             if (throwable != null)
                             {
                                 result.completeExceptionally(throwable);
                                 return;
                             }

                             try
                             {
                                 // combine future results into a single list
                                 // get is called, but we know all the futures have completed so it will not block
                                 result.complete(awaitAll(futures, false, (t) -> {
                                 }));
                             }
                             catch (final Throwable t)
                             {
                                 result.completeExceptionally(ThrowableUtils.rootCause(t));
                             }
                         });
        return result;
    }
}
