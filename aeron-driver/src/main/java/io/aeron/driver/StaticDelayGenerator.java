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

/**
 * Delay generator that simply returns a constant value (such as 0)
 */
public final class StaticDelayGenerator implements FeedbackDelayGenerator
{
    private final long delayInNs;
    private final boolean immediateFeedback;

    /**
     * Create a delayInNs generator that uses the specified delayInNs.
     *
     * @param delayInNs         to return
     * @param immediateFeedback or not
     */
    public StaticDelayGenerator(final long delayInNs, final boolean immediateFeedback)
    {
        this.delayInNs = delayInNs;
        this.immediateFeedback = immediateFeedback;
    }

    /**
     * {@inheritDoc}
     */
    public long generateDelay()
    {
        return delayInNs;
    }

    /**
     * {@inheritDoc}
     */
    public boolean shouldFeedbackImmediately()
    {
        return immediateFeedback;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "StaticDelayGenerator{" +
            "delayInNs=" + delayInNs +
            ", shouldFeedbackImmediately=" + shouldFeedbackImmediately() +
            '}';
    }
}
