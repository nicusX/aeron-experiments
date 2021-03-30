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

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Use the default host name resolver via {@link InetAddress}.
 */
public class DefaultNameResolver implements NameResolver
{
    /**
     * Singleton instance which can be used to avoid allocation.
     */
    public static final DefaultNameResolver INSTANCE = new DefaultNameResolver();

    /**
     * {@inheritDoc}
     */
    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        try
        {
            return InetAddress.getByName(name);
        }
        catch (final UnknownHostException ex)
        {
            return null;
        }
    }
}
