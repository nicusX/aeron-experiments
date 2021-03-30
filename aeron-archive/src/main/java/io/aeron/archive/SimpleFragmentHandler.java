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
package io.aeron.archive;

import org.agrona.concurrent.UnsafeBuffer;

@FunctionalInterface
interface SimpleFragmentHandler
{
    /**
     * Called by the {@link RecordingReader}. Implementors need to process DATA and PADDING fragments.
     *
     * @param buffer        containing the fragment.
     * @param offset        the data begins at.
     * @param length        length of the data.
     * @param frameType     to distinguish between DATA and PADDING fragments.
     * @param flags         flags for the frame.
     * @param reservedValue stored for the frame.
     */
    void onFragment(UnsafeBuffer buffer, int offset, int length, int frameType, byte flags, long reservedValue);
}
