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
package io.aeron.driver.media;

import org.junit.jupiter.api.Test;

import java.net.*;
import java.util.*;

import static java.lang.Short.parseShort;
import static java.net.InetAddress.getByName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static io.aeron.driver.media.NetworkUtil.filterBySubnet;
import static io.aeron.driver.media.NetworkUtil.isMatchWithPrefix;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NetworkUtilTest
{
    @Test
    public void shouldNotMatchIfLengthsAreDifferent()
    {
        assertFalse(isMatchWithPrefix(new byte[0], new byte[3], 0));
        assertFalse(isMatchWithPrefix(new byte[1], new byte[2], 0));
        assertFalse(isMatchWithPrefix(new byte[5], new byte[5000], 0));
    }

    @Test
    public void shouldMatchIfAllBytesMatch()
    {
        final byte[] a = { 'a', 'b', 'c', 'd' };
        final byte[] b = { 'a', 'b', 'c', 'd' };
        assertTrue(isMatchWithPrefix(a, b, 32));
    }

    @Test
    public void shouldMatchIfAllBytesWithPrefixMatch()
    {
        final byte[] a = { 'a', 'b', 'c', 'd' };
        final byte[] b = { 'a', 'b', 'c', 'e' };
        assertTrue(isMatchWithPrefix(a, b, 24));
    }

    @Test
    public void shouldNotMatchIfNotAllBytesWithPrefixMatch()
    {
        final byte[] a = { 'a', 'b', 'c', 'd' };
        final byte[] b = { 'a', 'b', 'd', 'd' };
        assertFalse(isMatchWithPrefix(a, b, 24));
    }

    @Test
    public void shouldMatchIfAllBytesWithPrefixUnalignedMatch()
    {
        assertTrue(isMatchWithPrefix(
            asBytes(0b10101010_11111111_00000000_00000000),
            asBytes(0b10101010_11111110_00000000_00000000),
            15));
    }

    @Test
    public void shouldNotMatchIfNotAllBytesWithUnalignedPrefixMatch()
    {
        assertFalse(isMatchWithPrefix(
            asBytes(0b10101010_11111111_00000000_00000000),
            asBytes(0b10101010_11111111_10000000_00000000),
            17));
    }

    @Test
    public void shouldFilterBySubnetAndFindOneResult() throws Exception
    {
        final NetworkInterfaceStub stub = new NetworkInterfaceStub();

        final NetworkInterface ifc1 = stub.add("192.168.0.1/24");
        stub.add("10.0.0.2/8");

        final NetworkInterface[] filteredBySubnet = filterBySubnet(stub, getByName("192.168.0.0"), 24);

        assertEquals(1, filteredBySubnet.length);
        assertEquals(ifc1, filteredBySubnet[0]);
    }

    @Test
    public void shouldFilterBySubnetAndFindNoResults() throws Exception
    {
        final NetworkInterfaceStub stub = new NetworkInterfaceStub();

        stub.add("192.168.0.1/24");
        stub.add("10.0.0.2/8");

        final NetworkInterface[] filteredBySubnet = filterBySubnet(stub, getByName("192.169.0.0"), 24);

        assertEquals(0, filteredBySubnet.length);
    }

    @Test
    public void shouldFilterBySubnetAndFindMultipleResultsOrderedByMatchLength() throws Exception
    {
        final NetworkInterfaceStub stub = new NetworkInterfaceStub();

        stub.add("10.0.0.2/8");
        final NetworkInterface ifc1 = stub.add("192.0.0.0/8");
        final NetworkInterface ifc2 = stub.add("192.168.1.1/24");
        final NetworkInterface ifc3 = stub.add("192.168.0.0/16");

        final NetworkInterface[] filteredBySubnet = filterBySubnet(stub, getByName("192.0.0.0"), 8);

        assertEquals(3, filteredBySubnet.length);
        assertThat(filteredBySubnet[0], sameInstance(ifc2));
        assertThat(filteredBySubnet[1], sameInstance(ifc3));
        assertThat(filteredBySubnet[2], sameInstance(ifc1));
    }

    @Test
    public void shouldFilterBySubnetAndFindOneIpV6Result() throws Exception
    {
        final NetworkInterfaceStub stub = new NetworkInterfaceStub();

        final NetworkInterface ifc1 = stub.add("fe80:0:0:0001:0002:0:0:1/80");
        stub.add("fe80:0:0:0002:0003:0:0:1/80");

        final NetworkInterface[] filteredBySubnet = filterBySubnet(stub, getByName("fe80:0:0:0001:0002:0:0:0"), 80);

        assertEquals(1, filteredBySubnet.length);
        assertEquals(ifc1, filteredBySubnet[0]);
    }

    @Test
    public void shouldFilterBySubnetAndFindNoIpV6Results() throws Exception
    {
        final NetworkInterfaceStub stub = new NetworkInterfaceStub();

        stub.add("fe80:0:0:0001:0:0:0:1/64");
        stub.add("fe80:0:0:0002:0:0:0:1/64");

        final NetworkInterface[] filteredBySubnet = filterBySubnet(stub, getByName("fe80:0:0:0004:0:0:0:0"), 64);

        assertEquals(0, filteredBySubnet.length);
    }

    @Test
    public void shouldFilterBySubnetAndFindMultipleIpV6ResultsOrderedByMatchLength() throws Exception
    {
        final NetworkInterfaceStub stub = new NetworkInterfaceStub();

        stub.add("ee80:0:0:0001:0:0:0:1/64");
        final NetworkInterface ifc1 = stub.add("fe80:0:0:0:0:0:0:1/16");
        final NetworkInterface ifc2 = stub.add("fe80:0001:0:0:0:0:0:1/32");
        final NetworkInterface ifc3 = stub.add("fe80:0001:abcd:0:0:0:0:1/48");

        final NetworkInterface[] filteredBySubnet = filterBySubnet(stub, getByName("fe80:0:0:0:0:0:0:0"), 16);

        assertEquals(3, filteredBySubnet.length);
        assertThat(filteredBySubnet[0], sameInstance(ifc3));
        assertThat(filteredBySubnet[1], sameInstance(ifc2));
        assertThat(filteredBySubnet[2], sameInstance(ifc1));
    }

    static class NetworkInterfaceStub implements NetworkInterfaceShim
    {
        private int counter = 0;

        private final IdentityHashMap<NetworkInterface, List<InterfaceAddress>> addressesByInterface =
            new IdentityHashMap<>();

        public Enumeration<NetworkInterface> getNetworkInterfaces()
        {
            return Collections.enumeration(addressesByInterface.keySet());
        }

        public List<InterfaceAddress> getInterfaceAddresses(final NetworkInterface ifc)
        {
            return addressesByInterface.get(ifc);
        }

        public boolean isLoopback(final NetworkInterface ifc)
        {
            return false;
        }

        public NetworkInterface add(final String... ips) throws UnknownHostException
        {
            final List<InterfaceAddress> ias = new ArrayList<>();
            for (final String ip : ips)
            {
                final String[] parts = ip.split("/");
                ias.add(newInterfaceAddress(getByName(parts[0]), parseShort(parts[1])));
            }

            final NetworkInterface ifc = newNetworkInterface(String.valueOf(counter++));
            addressesByInterface.put(ifc, ias);

            return ifc;
        }
    }

    private static NetworkInterface newNetworkInterface(final String name)
    {
        final NetworkInterface networkInterface = mock(NetworkInterface.class);

        when(networkInterface.getName()).thenReturn(name);

        return networkInterface;
    }

    private static InterfaceAddress newInterfaceAddress(final InetAddress inetAddress, final short maskLength)
    {
        final InterfaceAddress interfaceAddress = mock(InterfaceAddress.class);

        when(interfaceAddress.getAddress()).thenReturn(inetAddress);
        when(interfaceAddress.getNetworkPrefixLength()).thenReturn(maskLength);

        return interfaceAddress;
    }

    private static byte[] asBytes(final int i)
    {
        final byte[] bs = new byte[4];
        bs[0] = (byte)((i >> 24) & 0xFF);
        bs[1] = (byte)((i >> 16) & 0xFF);
        bs[2] = (byte)((i >> 8) & 0xFF);
        bs[3] = (byte)(i & 0xFF);

        return bs;
    }
}
