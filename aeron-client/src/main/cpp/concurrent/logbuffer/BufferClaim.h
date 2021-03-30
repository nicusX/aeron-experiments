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

#ifndef AERON_CONCURRENT_BUFFER_CLAIM_H
#define AERON_CONCURRENT_BUFFER_CLAIM_H

#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/DataFrameHeader.h"

namespace aeron { namespace concurrent { namespace logbuffer {

/**
 * Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
 * <p>
 * The claimed space is in {@link #buffer()} between {@link #offset()} and {@link #offset()} + {@link #length()}.
 * When the buffer is filled with message data, use {@link #commit()} to make it available to subscribers.
 */
class BufferClaim
{
public:
    typedef BufferClaim this_t;

    BufferClaim() = default;

    /// @cond HIDDEN_SYMBOLS
    inline void wrap(std::uint8_t *buffer, util::index_t length)
    {
        m_buffer.wrap(buffer, static_cast<std::size_t>(length));
    }
    /// @endcond

    /// @cond HIDDEN_SYMBOLS
    inline void wrap(AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        m_buffer.wrap(buffer.buffer() + offset, static_cast<std::size_t>(length));
    }
    /// @endcond


    /**
     * The referenced buffer to be used.
     *
     * @return the referenced buffer to be used..
     */
    inline AtomicBuffer &buffer()
    {
        return m_buffer;
    }

    /**
     * The offset in the buffer at which the claimed range begins.
     *
     * @return offset in the buffer at which the range begins.
     */
    inline util::index_t offset() const
    {
        return DataFrameHeader::LENGTH;
    }

    /**
     * The length of the claimed range in the buffer.
     *
     * @return length of the range in the buffer.
     */
    inline util::index_t length() const
    {
        return m_buffer.capacity() - DataFrameHeader::LENGTH;
    }

    /**
 * Get the value of the flags field.
 *
 * @return the value of the header flags field.
 */
    inline std::uint8_t flags() const
    {
        return m_buffer.getUInt8(DataFrameHeader::FLAGS_FIELD_OFFSET);
    }

    /**
     * Set the value of the header flags field.
     *
     * @param flags value to be set in the header.
     * @return this for a fluent API.
     */
    inline this_t& flags(const std::uint8_t flags)
    {
        m_buffer.putUInt8(DataFrameHeader::FLAGS_FIELD_OFFSET, flags);

        return *this;
    }

    /**
     * Get the value of the header type field.
     *
     * @return the value of the header type field.
     */
    inline std::uint16_t headerType() const
    {
        return m_buffer.getUInt16(DataFrameHeader::TYPE_FIELD_OFFSET);
    }

    /**
     * Set the value of the header type field.
     *
     * @param type value to be set in the header.
     * @return this for a fluent API.
     */
    inline this_t &headerType(const std::uint16_t type)
    {
        m_buffer.putUInt16(DataFrameHeader::TYPE_FIELD_OFFSET, type);

        return *this;
    }

    /**
     * Get the value stored in the reserve space at the end of a data frame header.
     *
     * @return the value stored in the reserve space at the end of a data frame header.
     */
    inline std::int64_t reservedValue() const
    {
        return m_buffer.getInt64(DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET);
    }

    /**
     * Write the provided value into the reserved space at the end of the data frame header.
     *
     * @param value to be stored in the reserve space at the end of a data frame header.
     * @return this for fluent API semantics.
     */
    inline this_t &reservedValue(const std::int64_t value)
    {
        m_buffer.putInt64(DataFrameHeader::RESERVED_VALUE_FIELD_OFFSET, value);
        return *this;
    }

    /**
     * Commit the message to the log buffer so that is it available to subscribers.
     */
    inline void commit()
    {
        m_buffer.putInt32Ordered(0, m_buffer.capacity());
    }

    /**
     * Abort a claim of the message space to the log buffer so that log can progress ignoring this claim.
     */
    inline void abort()
    {
        m_buffer.putUInt16(DataFrameHeader::TYPE_FIELD_OFFSET, DataFrameHeader::HDR_TYPE_PAD);
        m_buffer.putInt32Ordered(0, m_buffer.capacity());
    }

protected:
    AtomicBuffer m_buffer;
};

}}}

#endif
