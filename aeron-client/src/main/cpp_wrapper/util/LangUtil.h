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
#ifndef AERON_LANG_UTIL_H
#define AERON_LANG_UTIL_H

#include <functional>
#include <algorithm>

/**
 * @file
 * Utilities related to C++ and C++ standard libraries
 */
namespace aeron { namespace util
{

/**
 * Bjarne Stroustrup - Make Simple Tasks Simple - https://www.youtube.com/watch?v=nesCaocNjtQ
 */
template<typename T>
using Iterator = typename T::iterator;

template<typename Container, typename Predicate>
Iterator<Container> find_if(Container &c, Predicate p)
{
    return std::find_if(std::begin(c), std::end(c), p);
}

class InvokeOnScopeExit
{
public:
    using func_t = std::function<void()>;

    inline explicit InvokeOnScopeExit(const func_t &func) :
        m_func(func)
    {
    }

    inline ~InvokeOnScopeExit()
    {
        m_func();
    }

private:
    func_t m_func;
};

}}

#endif
