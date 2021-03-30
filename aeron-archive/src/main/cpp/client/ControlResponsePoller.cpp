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

#include "ControlResponsePoller.h"
#include "ArchiveException.h"
#include "aeron_archive_client/MessageHeader.h"
#include "aeron_archive_client/ControlResponse.h"
#include "aeron_archive_client/Challenge.h"

using namespace aeron;
using namespace aeron::archive::client;

static aeron::controlled_poll_fragment_handler_t controlHandler(ControlResponsePoller &poller)
{
    return
        [&](AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
        {
            return poller.onFragment(buffer, offset, length, header);
        };
}

ControlResponsePoller::ControlResponsePoller(std::shared_ptr<Subscription> subscription, int fragmentLimit) :
    m_fragmentAssembler(controlHandler(*this)),
    m_fragmentHandler(m_fragmentAssembler.handler()),
    m_subscription(std::move(subscription)),
    m_fragmentLimit(fragmentLimit)
{
}

ControlledPollAction ControlResponsePoller::onFragment(
    AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
{
    if (m_pollComplete)
    {
        return ControlledPollAction::ABORT;
    }

    MessageHeader msgHeader(
        buffer.sbeData() + offset,
        static_cast<std::uint64_t>(length),
        MessageHeader::sbeSchemaVersion());

    const std::int16_t schemaId = msgHeader.schemaId();
    if (schemaId != MessageHeader::sbeSchemaId())
    {
        throw ArchiveException(
            "expected schemaId=" + std::to_string(MessageHeader::sbeSchemaId()) +
            ", actual=" + std::to_string(schemaId),
            SOURCEINFO);
    }

    const uint16_t templateId = msgHeader.templateId();
    if (ControlResponse::sbeTemplateId() == templateId)
    {
        ControlResponse response(
            buffer.sbeData() + offset + MessageHeader::encodedLength(),
            static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
            msgHeader.blockLength(),
            msgHeader.version());

        m_controlSessionId = response.controlSessionId();
        m_correlationId = response.correlationId();
        m_relevantId = response.relevantId();
        m_version = response.version();

        ControlResponseCode::Value code = response.code();
        m_codeValue = code;
        m_isCodeError = ControlResponseCode::Value::ERROR == code;
        m_isCodeOk = ControlResponseCode::Value::OK == code;

        m_errorMessage = response.getErrorMessageAsString();

        m_isControlResponse = true;
        m_pollComplete = true;

        return ControlledPollAction::BREAK;
    }
    else if (Challenge::sbeTemplateId() == templateId)
    {
        Challenge response(
            buffer.sbeData() + offset + MessageHeader::encodedLength(),
            static_cast<std::uint64_t>(length) - MessageHeader::encodedLength(),
            msgHeader.blockLength(),
            msgHeader.version());

        m_controlSessionId = response.controlSessionId();
        m_correlationId = response.correlationId();
        m_relevantId = aeron::NULL_VALUE;
        m_version = response.version();

        m_codeValue = ControlResponseCode::NULL_VALUE;
        m_isCodeError = false;
        m_isCodeOk = false;
        m_errorMessage = "";

        const std::uint32_t encodedChallengeLength = response.encodedChallengeLength();
        char *encodedBuffer = new char[encodedChallengeLength];
        response.getEncodedChallenge(encodedBuffer, encodedChallengeLength);

        m_encodedChallenge.first = encodedBuffer;
        m_encodedChallenge.second = encodedChallengeLength;

        m_isControlResponse = false;
        m_wasChallenged = true;
        m_pollComplete = true;

        return ControlledPollAction::BREAK;
    }

    return ControlledPollAction::CONTINUE;
}
