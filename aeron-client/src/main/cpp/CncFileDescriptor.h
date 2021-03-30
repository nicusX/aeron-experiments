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

#ifndef AERON_CNC_FILE_DESCRIPTOR_H
#define AERON_CNC_FILE_DESCRIPTOR_H

#include "util/Index.h"
#include "util/MemoryMappedFile.h"
#include "util/MacroUtil.h"
#include "concurrent/AtomicBuffer.h"

namespace aeron
{

using namespace aeron::util;
using namespace aeron::concurrent;

/**
* Description of the command and control file used between driver and clients
*
* File Layout
* <pre>
*  +-----------------------------+
*  |          Meta Data          |
*  +-----------------------------+
*  |      to-driver Buffer       |
*  +-----------------------------+
*  |      to-clients Buffer      |
*  +-----------------------------+
*  |   Counters Metadata Buffer  |
*  +-----------------------------+
*  |    Counters Values Buffer   |
*  +-----------------------------+
*  |          Error Log          |
*  +-----------------------------+
* </pre>
* <p>
* Meta Data Layout {@link #CNC_VERSION}
* <pre>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |                      Aeron CnC Version                        |
*  +---------------------------------------------------------------+
*  |                   to-driver buffer length                     |
*  +---------------------------------------------------------------+
*  |                  to-clients buffer length                     |
*  +---------------------------------------------------------------+
*  |               Counters Metadata buffer length                 |
*  +---------------------------------------------------------------+
*  |                Counters Values buffer length                  |
*  +---------------------------------------------------------------+
*  |                   Error Log buffer length                     |
*  +---------------------------------------------------------------+
*  |                   Client Liveness Timeout                     |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                    Driver Start Timestamp                     |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                         Driver PID                            |
*  |                                                               |
*  +---------------------------------------------------------------+
* </pre>
*/
namespace CncFileDescriptor
{

static const std::string CNC_FILE = "cnc.dat";
static const std::int32_t CNC_VERSION = semanticVersionCompose(0, 2, 0);

#pragma pack(push)
#pragma pack(4)
struct MetaDataDefn
{
    std::int32_t cncVersion;
    std::int32_t toDriverBufferLength;
    std::int32_t toClientsBufferLength;
    std::int32_t counterMetadataBufferLength;
    std::int32_t counterValuesBufferLength;
    std::int32_t errorLogBufferLength;
    std::int64_t clientLivenessTimeout;
    std::int64_t startTimestamp;
    std::int64_t pid;
};
#pragma pack(pop)

static const std::size_t META_DATA_LENGTH = BitUtil::align(sizeof(MetaDataDefn), BitUtil::CACHE_LINE_LENGTH * 2);

inline static std::int32_t cncVersionVolatile(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));

    return metaDataBuffer.getInt32Volatile(static_cast<util::index_t>(offsetof(MetaDataDefn, cncVersion)));
}

inline static AtomicBuffer createToDriverBuffer(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();

    return AtomicBuffer(cncFile->getMemoryPtr() + META_DATA_LENGTH, metaData.toDriverBufferLength);
}

inline static AtomicBuffer createToClientsBuffer(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();
    std::uint8_t *basePtr = cncFile->getMemoryPtr() + META_DATA_LENGTH + metaData.toDriverBufferLength;

    return AtomicBuffer(basePtr, metaData.toClientsBufferLength);
}

inline static AtomicBuffer createCounterMetadataBuffer(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();
    std::uint8_t *basePtr =
        cncFile->getMemoryPtr() +
        META_DATA_LENGTH +
        metaData.toDriverBufferLength +
        metaData.toClientsBufferLength;

    return AtomicBuffer(basePtr, metaData.counterMetadataBufferLength);
}

inline static AtomicBuffer createCounterValuesBuffer(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();
    std::uint8_t *basePtr =
        cncFile->getMemoryPtr() +
        META_DATA_LENGTH +
        metaData.toDriverBufferLength +
        metaData.toClientsBufferLength +
        metaData.counterMetadataBufferLength;

    return AtomicBuffer(basePtr, metaData.counterValuesBufferLength);
}

inline static AtomicBuffer createErrorLogBuffer(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();
    std::uint8_t *basePtr =
        cncFile->getMemoryPtr() +
        META_DATA_LENGTH +
        metaData.toDriverBufferLength +
        metaData.toClientsBufferLength +
        metaData.counterMetadataBufferLength +
        metaData.counterValuesBufferLength;

    return AtomicBuffer(basePtr, metaData.errorLogBufferLength);
}

inline static std::int64_t clientLivenessTimeout(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();

    return metaData.clientLivenessTimeout;
}

inline static std::int64_t startTimestamp(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();

    return metaData.startTimestamp;
}

inline static std::int64_t pid(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();

    return metaData.pid;
}

inline static bool isCncFileLengthSufficient(MemoryMappedFile::ptr_t cncFile)
{
    AtomicBuffer metaDataBuffer(cncFile->getMemoryPtr(), convertSizeToIndex(cncFile->getMemorySize()));
    const MetaDataDefn &metaData = metaDataBuffer.overlayStruct<MetaDataDefn>();
    const std::size_t metadataRequiredLength =
        META_DATA_LENGTH +
        metaData.toDriverBufferLength +
        metaData.toClientsBufferLength +
        metaData.counterMetadataBufferLength +
        metaData.counterValuesBufferLength +
        metaData.errorLogBufferLength;

    return cncFile->getMemorySize() >= metadataRequiredLength;
}

}

}

#endif
