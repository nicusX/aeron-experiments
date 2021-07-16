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
package io.aeron.log;

import io.aeron.test.Tests;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit extension to start and reset the CollectingEventLogReaderAgent.
 */
public class EventLogExtension implements BeforeEachCallback, AfterEachCallback
{
    /**
     * {@inheritDoc}
     */
    public void beforeEach(final ExtensionContext context)
    {
        Tests.startLogCollecting();
    }

    /**
     * {@inheritDoc}
     */
    public void afterEach(final ExtensionContext context)
    {
        Tests.resetLogCollecting();
    }
}
