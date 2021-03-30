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

#include <cstdint>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "util/ScopeUtils.h"
#include "util/StringUtil.h"
#include "util/BitUtil.h"
#include "TestUtils.h"

using namespace aeron::util;

TEST(utilTests, scopeTest)
{
    bool flag = false;

    if (1)
    {
        OnScopeExit onExit([&]()
        {
            flag = true;
        });

        ASSERT_EQ(flag, false);
    }
    ASSERT_EQ(flag, true);
}

TEST(utilTests, stringUtilTrimTest)
{
    std::string test = "  test  ";

    ASSERT_EQ(trimWSLeft(test), "test  ");
    ASSERT_EQ(trimWSRight(test), "  test");
    ASSERT_EQ(trimWSBoth(test), "test");
}

TEST(utilTests, stringUtilParseTest)
{
    ASSERT_NO_THROW({
        ASSERT_EQ(parse<int>("100"), 100);
        ASSERT_EQ(parse<double>("100.25"), 100.25);
        ASSERT_EQ(parse<std::uint64_t>("0x123456789abcdef0"), 0x123456789abcdef0UL);
    });

    ASSERT_THROW(parse<int>(""), ParseException);
    ASSERT_THROW(parse<int>("  "), ParseException);
    ASSERT_THROW(parse<int>("xxx"), ParseException);
    ASSERT_THROW(parse<int>("84473.3443"), ParseException);
}

TEST(utilTests, stringUtilToStringTest)
{
    ASSERT_EQ(toString(100), "100");
    ASSERT_EQ(toString(1.25), "1.25");
    ASSERT_EQ(toString("hello"), "hello");
}

TEST(utilTests, stringUtilStrPrintfTest)
{
    std::string val = strPrintf("%s %s", "hello", "world");
    ASSERT_EQ(val, "hello world");
}

TEST(utilTests, findNextPowerOfTwo)
{
    EXPECT_EQ(BitUtil::findNextPowerOfTwo<std::uint32_t>(33), 64u);
    EXPECT_EQ(BitUtil::findNextPowerOfTwo<std::uint32_t>(4096), 4096u);
    EXPECT_EQ(BitUtil::findNextPowerOfTwo<std::uint32_t>(4097), 8192u);
}

TEST(utilTests, numberOfLeadingZeroes)
{
    EXPECT_EQ(BitUtil::numberOfLeadingZeroes<std::uint32_t>(0xFFFFFFFF), 0);
    EXPECT_EQ(BitUtil::numberOfLeadingZeroes<std::uint32_t>(0x10000000), 3);
    EXPECT_EQ(BitUtil::numberOfLeadingZeroes<std::uint32_t>(0x010000FF), 7);
    EXPECT_EQ(BitUtil::numberOfLeadingZeroes<std::uint32_t>(0x0000FFFF), 16);
    EXPECT_EQ(BitUtil::numberOfLeadingZeroes<std::uint32_t>(0x00000001), 31);
}

TEST(utilTests, numberOfTrailingZeroes)
{
    EXPECT_EQ(BitUtil::numberOfTrailingZeroes<std::uint32_t>(1 << 21), 21);
    EXPECT_EQ(BitUtil::numberOfTrailingZeroes<std::uint32_t>(0x00000008), 3);
    EXPECT_EQ(BitUtil::numberOfTrailingZeroes<std::uint32_t>(0x80000000), 31);
    EXPECT_EQ(BitUtil::numberOfTrailingZeroes<std::uint32_t>(0x01000080), 7);
    EXPECT_EQ(BitUtil::numberOfTrailingZeroes<std::uint32_t>(0x0000FFFF), 0);
    EXPECT_EQ(BitUtil::numberOfTrailingZeroes<std::uint32_t>(0xFFFF0000), 16);
    EXPECT_EQ(BitUtil::numberOfTrailingZeroes<std::uint32_t>(0x00000001), 0);
}

void throwIllegalArgumentException()
{
    aeron::test::throwIllegalArgumentException();
}

TEST(utilTests, sourcedException)
{
#if defined(_MSC_VER)
    const std::string aeron_client_dir = " aeron-client\\";
#else
    const std::string aeron_client_dir = " aeron-client/";
#endif

#if defined(_MSC_VER) && defined(MSVC_FILE_IS_LOWER_CASE)
    const std::string testutils_h_filename = "testutils.h";
#else
    const std::string testutils_h_filename = "TestUtils.h";
#endif

    EXPECT_THROW({
        try
        {
            aeron::test::throwIllegalArgumentException();
        }
        catch (const SourcedException &e)
        {
            // Path must be relative and not have a prefix
            EXPECT_THAT(e.where(), ::testing::HasSubstr(aeron_client_dir));
            // The exception should point to the code before it was inlined
            EXPECT_THAT(e.where(), ::testing::HasSubstr(testutils_h_filename));
            throw;
        }
    }, SourcedException);
}
