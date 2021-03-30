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
package io.aeron.agent;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.agent.ArchiveEventCode.EVENT_CODE_TYPE;
import static io.aeron.agent.ArchiveEventCode.fromEventCodeId;
import static org.junit.jupiter.api.Assertions.*;

public class ArchiveEventCodeTest
{
    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void getCodeById(final ArchiveEventCode code)
    {
        assertSame(code, ArchiveEventCode.get(code.id()));
    }

    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void toEventCodeIdComputesEventId(final ArchiveEventCode eventCode)
    {
        assertEquals((EVENT_CODE_TYPE << 16) | (0xFFFF & eventCode.id()), eventCode.toEventCodeId());
    }

    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void fromEventCodeIdLooksUpEventCode(final ArchiveEventCode eventCode)
    {
        assertSame(eventCode, fromEventCodeId(eventCode.toEventCodeId()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, 13, Integer.MIN_VALUE, Integer.MAX_VALUE })
    void fromEventCodeIdReturnNullForUnknownCode(final int eventCodeId)
    {
        assertNull(fromEventCodeId(eventCodeId));
    }
}
