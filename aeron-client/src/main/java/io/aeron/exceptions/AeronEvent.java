/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.exceptions;

/**
 * A means to capture an event of significance that does not require a stack trace so it can be lighter weight
 * and take up less space in a {@link org.agrona.concurrent.errors.DistinctErrorLog}.
 */
public class AeronEvent extends AeronException
{
    /**
     * Aeron event with provided message and {@link AeronException.Category#WARN}.
     *
     * @param message to detail the event.
     */
    public AeronEvent(final String message)
    {
        super(message, AeronException.Category.WARN);
    }

    /**
     * Aeron event with provided message and {@link AeronException.Category}.
     *
     * @param message  to detail the event.
     * @param category of the event.
     */
    public AeronEvent(final String message, final AeronException.Category category)
    {
        super(message, category);
    }

    /**
     * Override the base implementation so no stack trace is associated.
     * <p>
     * <b>Note:</b> This method is not synchronized as it does not call the super class.
     *
     * @return a reference to this {@link AeronEvent} instance.
     */
    @SuppressWarnings("lgtm[java/non-sync-override]")
    public Throwable fillInStackTrace()
    {
        return this;
    }
}
