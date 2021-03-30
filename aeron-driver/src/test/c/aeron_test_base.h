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

#ifndef AERON_AERON_TEST_BASE_H
#define AERON_AERON_TEST_BASE_H

#include "aeronc.h"
#include "EmbeddedMediaDriver.h"

extern "C"
{
#include "util/aeron_env.h"
}

class CSystemTestBase
{
public:
    using poll_handler_t = std::function<void(const uint8_t *, size_t, aeron_header_t *)>;
    using image_handler_t = std::function<void(aeron_subscription_t *, aeron_image_t *)>;

    CSystemTestBase(std::vector<std::pair<std::string, std::string>> environment = {})
    {
        for (auto &envVar : environment)
        {
            aeron_env_set(envVar.first.data(), envVar.second.data());
        }
        m_driver.start();
    }

    virtual ~CSystemTestBase()
    {
        if (m_aeron)
        {
            aeron_close(m_aeron);
        }

        if (m_context)
        {
            aeron_context_close(m_context);
        }

        m_driver.stop();
    }

    virtual aeron_t *connect()
    {
        if (aeron_context_init(&m_context) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        if (aeron_init(&m_aeron, m_context) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        if (aeron_start(m_aeron) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        return m_aeron;
    }

    static aeron_publication_t *awaitPublicationOrError(aeron_async_add_publication_t *async)
    {
        aeron_publication_t *publication = nullptr;

        do
        {
            std::this_thread::yield();
            if (aeron_async_add_publication_poll(&publication, async) < 0)
            {
                return nullptr;
            }
        }
        while (!publication);

        return publication;
    }

    static aeron_exclusive_publication_t *awaitExclusivePublicationOrError(
        aeron_async_add_exclusive_publication_t *async)
    {
        aeron_exclusive_publication_t *publication = nullptr;

        do
        {
            std::this_thread::yield();
            if (aeron_async_add_exclusive_publication_poll(&publication, async) < 0)
            {
                return nullptr;
            }
        }
        while (!publication);

        return publication;
    }

    static aeron_subscription_t *awaitSubscriptionOrError(aeron_async_add_subscription_t *async)
    {
        aeron_subscription_t *subscription = nullptr;

        do
        {
            std::this_thread::yield();
            if (aeron_async_add_subscription_poll(&subscription, async) < 0)
            {
                return nullptr;
            }
        }
        while (!subscription);

        return subscription;
    }

    static aeron_counter_t *awaitCounterOrError(aeron_async_add_counter_t *async)
    {
        aeron_counter_t *counter = nullptr;

        do
        {
            std::this_thread::yield();
            if (aeron_async_add_counter_poll(&counter, async) < 0)
            {
                return nullptr;
            }
        }
        while (!counter);

        return counter;
    }

    static void awaitConnected(aeron_subscription_t *subscription)
    {
        while (!aeron_subscription_is_connected(subscription))
        {
            std::this_thread::yield();
        }
    }

    static void poll_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
        auto test = reinterpret_cast<CSystemTestBase *>(clientd);

        test->m_poll_handler(buffer, length, header);
    }

    int poll(aeron_subscription_t *subscription, poll_handler_t &handler, int fragment_limit)
    {
        m_poll_handler = handler;
        return aeron_subscription_poll(subscription, poll_handler, this, (size_t)fragment_limit);
    }

    static void onUnavailableImage(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
    {
        auto test = reinterpret_cast<CSystemTestBase *>(clientd);

        if (test->m_onUnavailableImage)
        {
            test->m_onUnavailableImage(subscription, image);
        }
    }

    static void setFlagOnClose(void *clientd)
    {
        std::atomic<bool> *flag = static_cast<std::atomic<bool> *>(clientd);
        flag->store(true);
    }

protected:
    aeron::EmbeddedMediaDriver m_driver;
    aeron_context_t *m_context = nullptr;
    aeron_t *m_aeron = nullptr;

    poll_handler_t m_poll_handler = nullptr;
    image_handler_t m_onUnavailableImage = nullptr;
};

#endif //AERON_AERON_TEST_BASE_H
