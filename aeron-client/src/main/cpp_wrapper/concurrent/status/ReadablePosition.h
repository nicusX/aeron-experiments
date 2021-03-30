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

#ifndef AERON_READONLY_POSITION_H
#define AERON_READONLY_POSITION_H

#include <cstdint>

namespace aeron { namespace concurrent { namespace status {

template <class X>
class ReadablePosition
{
public:
    explicit ReadablePosition(X &impl) : m_impl(impl)
    {
    }

    inline void wrap(const ReadablePosition<X> &position)
    {
        m_impl.wrap(position.m_impl);
    }

    inline std::int32_t id() const
    {
        return m_impl.id();
    }

    inline std::int64_t get() const
    {
        return m_impl.get();
    }

    inline std::int64_t getVolatile() const
    {
        return m_impl.getVolatile();
    }

    inline void close()
    {
        m_impl.close();
    }

protected:
    X m_impl;
};

}}}

#endif //AERON_READONLY_POSITION_H
