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
package io.aeron.driver;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class OptimalMulticastDelayGeneratorTest
{
    private static final long MAX_BACKOFF = TimeUnit.MILLISECONDS.toNanos(60);
    private static final long GROUP_SIZE = 10;

    private final OptimalMulticastDelayGenerator generator = new OptimalMulticastDelayGenerator(
        MAX_BACKOFF, GROUP_SIZE);

    @Test
    public void shouldNotExceedTmaxBackoff()
    {
        for (int i = 0; i < 100_000; i++)
        {
            final double delay = generator.generateNewOptimalDelay();
            assertThat(delay, lessThanOrEqualTo((double)MAX_BACKOFF));
        }
    }
}
