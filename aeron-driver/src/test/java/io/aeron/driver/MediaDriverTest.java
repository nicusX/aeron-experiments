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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class MediaDriverTest
{
    @Test
    public void shouldPrintConfigOnStart()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .printConfigurationOnStart(true);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream(os);
        final PrintStream out = System.out;
        System.setOut(printStream);

        try (MediaDriver ignore = MediaDriver.launch(context))
        {
            final String result = os.toString();
            assertThat(result, containsString("printConfigurationOnStart=true"));
        }
        finally
        {
            System.setOut(out);
        }
    }
}