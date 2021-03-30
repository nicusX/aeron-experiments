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
#ifndef AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H
#define AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H

#include <utility>

#include "Aeron.h"
#include "ControlledFragmentAssembler.h"

namespace aeron { namespace archive { namespace client
{

/**
 * Encapsulate the polling and decoding of archive control protocol response messages.
 */
class ControlResponsePoller
{
public:
    explicit ControlResponsePoller(std::shared_ptr<Subscription> subscription, int fragmentLimit = 10);

    /**
     * Get the Subscription used for polling responses.
     *
     * @return the Subscription used for polling responses.
     */
    inline std::shared_ptr<Subscription> subscription()
    {
        return m_subscription;
    }

    /**
     * Poll for control response events.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    inline int poll()
    {
        m_controlSessionId = -1;
        m_correlationId = -1;
        m_relevantId = -1;
        m_version = 0;
        m_codeValue = -1;
        m_errorMessage = "";
        m_pollComplete = false;
        m_isCodeOk = false;
        m_isCodeError = false;
        m_isControlResponse = false;
        m_wasChallenged = false;
        delete[] m_encodedChallenge.first;
        m_encodedChallenge.first = nullptr;
        m_encodedChallenge.second = 0;

        return m_subscription->controlledPoll(m_fragmentHandler, m_fragmentLimit);
    }

    /**
     * Control session id of the last polled message or Aeron#NULL_VALUE if poll returned nothing.
     *
     * @return control session id of the last polled message or Aeron#NULL_VALUE if poll returned nothing.
     */
    inline std::int64_t controlSessionId()
    {
        return m_controlSessionId;
    }

    /**
     * Correlation id of the last polled message or Aeron#NULL_VALUE if poll returned nothing.
     *
     * @return correlation id of the last polled message or Aeron#NULL_VALUE if poll returned nothing.
     */
    inline std::int64_t correlationId()
    {
        return m_correlationId;
    }

    /**
     * Get the relevant id returned with the response, e.g. replay session id.
     *
     * @return the relevant id returned with the response.
     */
    inline std::int64_t relevantId()
    {
        return m_relevantId;
    }

    /**
     * Version response from the server in semantic version form.
     *
     * @return response from the server in semantic version form.
     */
    inline std::int32_t version()
    {
        return m_version;
    }

    /**
     * Was last received message a Control Response?
     *
     * @return whether the last received message was a Control Response.
     */
    inline bool isControlResponse()
    {
        return m_isControlResponse;
    }

    /**
     * Was the last polling action received a complete message?
     *
     * @return true if the last polling action received a complete message?
     */
    inline bool isPollComplete()
    {
        return m_pollComplete;
    }

    /**
     * Get the error message of the last response.
     *
     * @return the error message of the last response.
     */
    inline std::string errorMessage()
    {
        return m_errorMessage;
    }

    /**
     * Did the last received control response have a response code of OK?
     *
     * @return whether the last received control response had a response code of OK?
     */
    inline bool isCodeOk()
    {
        return m_isCodeOk;
    }

    /**
     * Did the last received control response have a response code of ERROR?
     *
     * @return whether the last received control response had a response code of ERROR?
     */
    inline bool isCodeError()
    {
        return m_isCodeError;
    }

    /**
     * Get the response code value of the last response.
     *
     * @return the response code value of the last response.
     */
    inline int codeValue()
    {
        return m_codeValue;
    }

    /**
     * Was the last polling action received a challenge message?
     *
     * @return true if the last polling action received was a challenge message, false if not.
     */
    inline bool wasChallenged()
    {
        return m_wasChallenged;
    }

    /**
     * Get the encoded challenge of the last challenge.
     *
     * @return the encoded challenge of the last challenge.
     */
    inline std::pair<const char *, std::uint32_t> encodedChallenge()
    {
        return m_encodedChallenge;
    }

    ControlledPollAction onFragment(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header);

private:
    ControlledFragmentAssembler m_fragmentAssembler;
    controlled_poll_fragment_handler_t m_fragmentHandler;
    std::shared_ptr<Subscription> m_subscription;
    const int m_fragmentLimit;

    std::int64_t m_controlSessionId = -1;
    std::int64_t m_correlationId = -1;
    std::int64_t m_relevantId = -1;
    std::int32_t m_version = 0;
    int m_codeValue = -1;
    std::string m_errorMessage = "";
    bool m_pollComplete = false;
    bool m_isCodeOk = false;
    bool m_isCodeError = false;
    bool m_isControlResponse = false;
    bool m_wasChallenged = false;
    std::pair<const char *, std::uint32_t> m_encodedChallenge = { nullptr, 0 };
};

}}}

#endif //AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H
