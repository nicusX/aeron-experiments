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
package io.aeron.samples;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class LogInspectorAsciiFormatBytesTest
{
    private String originalDataFormatProperty;

    private static Stream<Arguments> data()
    {
        return Stream.of(
            arguments((byte)0x17, (char)0x17),
            arguments((byte)0, (char)0),
            arguments((byte)-1, (char)0),
            arguments(Byte.MAX_VALUE, (char)Byte.MAX_VALUE),
            arguments(Byte.MIN_VALUE, (char)0));
    }

    @BeforeEach
    public void before()
    {
        originalDataFormatProperty = System.getProperty(LogInspector.AERON_LOG_DATA_FORMAT_PROP_NAME);
    }

    @AfterEach
    public void after()
    {
        if (null == originalDataFormatProperty)
        {
            System.clearProperty(LogInspector.AERON_LOG_DATA_FORMAT_PROP_NAME);
        }
        else
        {
            System.setProperty(LogInspector.AERON_LOG_DATA_FORMAT_PROP_NAME, originalDataFormatProperty);
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void shouldFormatBytesToAscii(final byte buffer, final char expected)
    {
        System.setProperty(LogInspector.AERON_LOG_DATA_FORMAT_PROP_NAME, "ascii");
        final char[] formattedBytes = LogInspector.formatBytes(new UnsafeBuffer(new byte[]{ buffer }), 0, 1);

        assertEquals(expected, formattedBytes[0]);
    }
}