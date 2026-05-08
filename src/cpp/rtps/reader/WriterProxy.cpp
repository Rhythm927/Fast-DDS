// Copyright 2016 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file WriterProxy.cpp
 *
 */

#include <rtps/reader/WriterProxy.h>

#include <fastdds/dds/log/Log.hpp>
#include <fastdds/rtps/writer/RTPSWriter.hpp>

#include "rtps/domain/RTPSDomainImpl.hpp"
#include "utils/collections/node_size_helpers.hpp"
#include <rtps/builtin/data/WriterProxyData.hpp>
#include <rtps/messages/RTPSMessageCreator.hpp>
#include <rtps/network/utils/external_locators.hpp>
#include <rtps/participant/RTPSParticipantImpl.hpp>
#include <rtps/participant/RTPSParticipantImpl.hpp>
#include <rtps/resources/TimedEvent.h>
#include <rtps/reader/StatefulReader.hpp>
#include <rtps/writer/BaseWriter.hpp>

#if !defined(NDEBUG) && !defined(ANDROID) && defined(FASTDDS_SOURCE) && defined(__unix__)
#define SHOULD_DEBUG_LINUX
#endif // SHOULD_DEBUG_LINUX

#ifdef SHOULD_DEBUG_LINUX
#include <sys/syscall.h>
#include <unistd.h>
#include <pthread.h>
#include <cassert>
#include <mutex>
#endif // SHOULD_DEBUG_LINUX

namespace eprosima {
namespace fastdds {
namespace rtps {

WriterProxy::~WriterProxy()
{
    if (is_alive_ && is_on_same_process_)
    {
        EPROSIMA_LOG_WARNING(RTPS_READER, "Automatically unmatching on ~WriterProxy");
        BaseWriter* writer = RTPSDomainImpl::get_instance()->find_writer(guid());
        if (writer)
        {
            writer->matched_reader_remove(reader_->getGuid());
        }
    }

    delete(initial_acknack_);
    delete(heartbeat_response_);
}

using set_helper = utilities::collections::set_size_helper<SequenceNumber_t>;

WriterProxy::WriterProxy(
        StatefulReader* reader,
        const RemoteLocatorsAllocationAttributes& loc_alloc,
        const ResourceLimitedContainerConfig& changes_allocation)
    : reader_(reader)
    , heartbeat_response_(nullptr)
    , initial_acknack_(nullptr)
    , last_heartbeat_count_(0)
    , heartbeat_final_flag_(false)
    , is_alive_(false)
    , changes_pool_(
        set_helper::node_size,
        set_helper::min_pool_size<pool_allocator_t>(changes_allocation.initial))
    , changes_received_(changes_pool_)
    , guid_as_vector_(ResourceLimitedContainerConfig::fixed_size_configuration(1u))
    , guid_prefix_as_vector_(ResourceLimitedContainerConfig::fixed_size_configuration(1u))
    , is_on_same_process_(false)
    , ownership_strength_(0)
    , liveliness_kind_(dds::AUTOMATIC_LIVELINESS_QOS)
    , locators_entry_(loc_alloc.max_unicast_locators, loc_alloc.max_multicast_locators)
    , is_datasharing_writer_(false)
    , received_at_least_one_heartbeat_(false)
    , state_(StateCode::STOPPED)
{
    //Create Events
    ResourceEvent& event_manager = reader_->getEventResource();
    auto heartbeat_lambda = [this]() -> bool
            {
                perform_heartbeat_response();
                return false;
            };
    auto acknack_lambda = [this]() -> bool
            {
                return perform_initial_ack_nack();
            };

    heartbeat_response_ = new TimedEvent(event_manager, heartbeat_lambda, 0);
    initial_acknack_ = new TimedEvent(event_manager, acknack_lambda, 0);

    clear();
    EPROSIMA_LOG_INFO(RTPS_READER, "Writer Proxy created in reader: " << reader_->getGuid().entityId);
}

void WriterProxy::start(
        const WriterProxyData& attributes,
        const SequenceNumber_t& initial_sequence)
{
    start(attributes, initial_sequence, false);
}

void WriterProxy::start(
        const WriterProxyData& attributes,
        const SequenceNumber_t& initial_sequence,
        bool is_datasharing)
{
    using network::external_locators::filter_remote_locators;

#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX
    //设置了响应时间，就是在收到EDP的heartbeat消息，之后多久会发送response 5ms
    heartbeat_response_->update_interval(reader_->getTimes().heartbeat_response_delay);
    //设置了时间，就是在initial_acknack_启动之后多久会发送initialacknack的消息 这里面默认是70ms
    initial_acknack_->update_interval(reader_->getTimes().initial_acknack_delay);

    locators_entry_.remote_guid = attributes.guid;
    guid_as_vector_.push_back(attributes.guid);
    guid_prefix_as_vector_.push_back(attributes.guid.guidPrefix);
    persistence_guid_ = attributes.persistence_guid;
    is_alive_ = true;
    is_on_same_process_ = RTPSDomainImpl::should_intraprocess_between(reader_->getGuid(), attributes.guid);
    ownership_strength_ = attributes.ownership_strength.value;
    liveliness_kind_ = attributes.liveliness.kind;
    locators_entry_.unicast = attributes.remote_locators.unicast;
    locators_entry_.multicast = attributes.remote_locators.multicast;
    filter_remote_locators(locators_entry_,
            reader_->getAttributes().external_unicast_locators, reader_->getAttributes().ignore_non_matching_locators);
    is_datasharing_writer_ = is_datasharing;
    state_.store(StateCode::IDLE);
    //这里面启动了timedevent，这个可以参照之前的timedevent
    initial_acknack_->restart_timer();
    loaded_from_storage(initial_sequence);
    received_at_least_one_heartbeat_ = false;
}

void WriterProxy::update(
        const WriterProxyData& attributes)
{
    using network::external_locators::filter_remote_locators;

#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    assert(is_alive_);
    ownership_strength_ = attributes.ownership_strength.value;
    locators_entry_.unicast = attributes.remote_locators.unicast;
    locators_entry_.multicast = attributes.remote_locators.multicast;
    filter_remote_locators(locators_entry_,
            reader_->getAttributes().external_unicast_locators, reader_->getAttributes().ignore_non_matching_locators);
}

void WriterProxy::stop()
{
    StateCode prev_code;
    if ((prev_code = state_.exchange(StateCode::STOPPED)) == StateCode::BUSY)
    {
        // TimedEvent being performed, wait for it to finish.
        // It does not matter which of the two events is the one on execution, but we must wait on initial_acknack_ as
        // it could be restarted if only cancelled while its callback is being triggered.
        initial_acknack_->recreate_timer();
    }
    else
    {
        initial_acknack_->cancel_timer();
    }
    heartbeat_response_->cancel_timer();

    clear();
}

void WriterProxy::clear()
{
    is_alive_ = false;
    locators_entry_.unicast.clear();
    locators_entry_.multicast.clear();
    locators_entry_.remote_guid = c_Guid_Unknown;
    last_heartbeat_count_ = 0;
    heartbeat_final_flag_.store(false);
    guid_as_vector_.clear();
    guid_prefix_as_vector_.clear();
    changes_received_.clear();
    is_on_same_process_ = false;
    loaded_from_storage(SequenceNumber_t());
}

void WriterProxy::loaded_from_storage(
        const SequenceNumber_t& seq_num)
{
    last_notified_ = seq_num;
    changes_from_writer_low_mark_ = seq_num;
    max_sequence_number_ = seq_num;
}

void WriterProxy::missing_changes_update(
        const SequenceNumber_t& seq_num)
{
    // 知道我们还有多少msg能够收到，但是却没有收到
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    EPROSIMA_LOG_INFO(RTPS_READER, guid().entityId << ": changes up to seq_num: " << seq_num << " missing.");

    // Check was not removed from container.
    if (seq_num > changes_from_writer_low_mark_)
    {
        if (seq_num > max_sequence_number_)
        {
            // 发送端StatefulWriter中消息seq的最大值
            max_sequence_number_ = seq_num;
        }
    }
}

int32_t WriterProxy::lost_changes_update(
        const SequenceNumber_t& seq_num)
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX
    // 收到 Heartbeat 后，根据 heartbeat 里的 firstSN 
    // 判断哪些旧序号已经不可能再收到，把它们标记为 lost，并推进 low mark。

    EPROSIMA_LOG_INFO(RTPS_READER, guid().entityId << ": up to seq_num: " << seq_num);
    int32_t current_sample_lost = 0;

    // Check was not removed from container
    // 这个 writer 到哪个序号为止，reader 已经不再关心了。
    // 如果writer端最小的seq比changes_from_writer_low_mark_+1大
    if (seq_num > (changes_from_writer_low_mark_ + 1))
    {
        // Remove all received changes with a sequence lower than seq_num
        // changes_received_:  在 low_mark 之后，已经收到的非连续序号集合。

        ///@note 计算[changes_from_writer_low_mark_ + 1, seq_num) 这个区间里，有多少个序号没有收到，也就是 lost sample 数量。
        // it 指向第一个 >= seq_num 的已收到序号
        ChangeIterator it = std::lower_bound(changes_received_.begin(), changes_received_.end(), seq_num);
        if (!changes_received_.empty())
        {
            //这个tmp表示的是 changes_received_中第一个message到changes_from_writer_low_mark_的长度
            uint64_t tmp = (*changes_received_.begin()).to64long() - (changes_from_writer_low_mark_.to64long() + 1);
            auto distance = std::distance(changes_received_.begin(), it);
            tmp += seq_num.to64long() - (*changes_received_.begin()).to64long() - distance;
            current_sample_lost = tmp > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ?
                    std::numeric_limits<int32_t>::max() : static_cast<int32_t>(tmp);
        }
        else   // 没有收到消息，那么候选区间全部都算成lost
        {
            uint64_t tmp = seq_num.to64long() - (changes_from_writer_low_mark_.to64long() + 1);
            current_sample_lost = tmp > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ?
                    std::numeric_limits<int32_t>::max() : static_cast<int32_t>(tmp);
        }
        changes_received_.erase(changes_received_.begin(), it);

        // Update low mark
        changes_from_writer_low_mark_ = seq_num - 1;
        if (changes_from_writer_low_mark_ > max_sequence_number_)
        {
            max_sequence_number_ = changes_from_writer_low_mark_;
        }

        // Next could need to be removed.
        cleanup();
    }

    return current_sample_lost;
}

bool WriterProxy::received_change_set(
        const SequenceNumber_t& seq_num)
{
    EPROSIMA_LOG_INFO(RTPS_READER, guid().entityId << ": seq_num: " << seq_num);
    return received_change_set(seq_num, true);
}

bool WriterProxy::irrelevant_change_set(
        const SequenceNumber_t& seq_num)
{
    return received_change_set(seq_num, false);
}

bool WriterProxy::received_change_set(
        const SequenceNumber_t& seq_num,
        bool /* is_relevance */ )
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    // Check if CacheChange_t was already and it was already removed from changesFromW container.
    if (seq_num <= changes_from_writer_low_mark_)
    {
        EPROSIMA_LOG_INFO(RTPS_READER, "Change " << seq_num << " <= than max available sequence number "
                                                 << changes_from_writer_low_mark_);
        return false;
    }

    // If will be the last element, insert it at the end.
    if (seq_num > max_sequence_number_)
    {
        // If it is the next to be acknowledeg, not insert
        if (seq_num == changes_from_writer_low_mark_ + 1)
        {
            changes_from_writer_low_mark_ = seq_num;
        }
        else
        {
            changes_received_.insert(changes_received_.end(), seq_num);
        }
        max_sequence_number_ = seq_num;
    }
    else
    {
        // Check if it is next to the last acknowledged
        if (changes_from_writer_low_mark_ + 1 == seq_num)
        {
            changes_from_writer_low_mark_ = seq_num;
            cleanup();
        }
        else
        {
            // Check if already received
            if (changes_received_.find(seq_num) != changes_received_.end())
            {
                return false;
            }

            changes_received_.insert(seq_num);
        }
    }

    return true;
}

SequenceNumberSet_t WriterProxy::missing_changes() const
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    SequenceNumber_t first_missing = changes_from_writer_low_mark_ + 1;
    SequenceNumber_t max_missing = std::min(first_missing + 256UL, max_sequence_number_ + 1);
    SequenceNumberSet_t sns(first_missing);

    for (SequenceNumber_t seq : changes_received_)
    {
        seq = std::min(seq, max_missing);
        sns.add_range(first_missing, seq);
        first_missing = seq + 1;
        if (first_missing >= max_missing)
        {
            break;
        }
    }

    if (first_missing < max_missing)
    {
        sns.add_range(first_missing, max_missing);
    }

    return sns;
}

bool WriterProxy::change_was_received(
        const SequenceNumber_t& seq_num) const
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    if (seq_num <= changes_from_writer_low_mark_)
    {
        return true;
    }

    ChangeIterator chit = changes_received_.find(seq_num);
    return chit != changes_received_.end();
}

const SequenceNumber_t WriterProxy::available_changes_max() const
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    return changes_from_writer_low_mark_;
}

void WriterProxy::cleanup()
{
    ChangeIterator chit = changes_received_.begin();

    // Jump over all consecutive received changes starting on the next to low_mark
    while (chit != changes_received_.end() && *chit == changes_from_writer_low_mark_ + 1)
    {
        chit++;
        changes_from_writer_low_mark_++;
    }

    // Remove all those changes
    changes_received_.erase(changes_received_.begin(), chit);
}

bool WriterProxy::are_there_missing_changes() const
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    return changes_from_writer_low_mark_ < max_sequence_number_;
}

size_t WriterProxy::unknown_missing_changes_up_to(
        const SequenceNumber_t& seq_num) const
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    uint32_t returnedValue = 0;

    if (seq_num > changes_from_writer_low_mark_)
    {
        SequenceNumber_t first_missing = changes_from_writer_low_mark_ + 1;
        SequenceNumberSet_t sns(first_missing);
        SequenceNumberDiff d_fun;

        for (SequenceNumber_t seq : changes_received_)
        {
            seq = std::min(seq, seq_num);
            if (first_missing < seq)
            {
                returnedValue += d_fun(seq, first_missing);
            }
            first_missing = seq + 1;
            if (first_missing >= seq_num)
            {
                break;
            }
        }

        if (first_missing < seq_num)
        {
            returnedValue += d_fun(seq_num, first_missing);
        }
    }

    return returnedValue;
}

size_t WriterProxy::number_of_changes_from_writer() const
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    if (max_sequence_number_ > changes_from_writer_low_mark_)
    {
        SequenceNumberDiff d_fun;
        return d_fun(max_sequence_number_, changes_from_writer_low_mark_);
    }

    return 0;
}

SequenceNumber_t WriterProxy::next_cache_change_to_be_notified()
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    if (last_notified_ < changes_from_writer_low_mark_)
    {
        ++last_notified_;
        return last_notified_;
    }

    return SequenceNumber_t::unknown();
}

void WriterProxy::consider_all_notified()
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    if (last_notified_ < changes_from_writer_low_mark_)
    {
        last_notified_ = changes_from_writer_low_mark_;
    }
}

bool WriterProxy::perform_initial_ack_nack()
{
    bool ret_value = false;
    // compare_exchange_strong(expected, BUSY)：只有当 state_ == IDLE 时，才把它原子地改成 BUSY 并返回 true
    StateCode expected = StateCode::IDLE;
    // 成功：本线程把状态从 IDLE 置为 BUSY，获得“执行权”，可以继续往下做事
    // 失败：当前状态不是 IDLE，本线程直接退出（不阻塞）
    if (!state_.compare_exchange_strong(expected, StateCode::BUSY))
    {
        // Stopped from another thread -> abort
        return ret_value;
    }
    // IDLE -> BUSY 之后
    if (!is_datasharing_writer_)
    {
        // Send initial NACK.
        SequenceNumberSet_t sns(SequenceNumber_t(0, 0));
        // 同进程
        if (is_on_same_process_)
        {
            BaseWriter* writer = RTPSDomainImpl::get_instance()->find_writer(guid());
            if (writer)
            {
                bool tmp;
                writer->process_acknack(guid(), reader_->getGuid(), 1,
                        SequenceNumberSet_t(), false, tmp, c_VendorId_eProsima);
            }
        }
        else
        {
            // 不是一个进程的，如果没有收到过心跳，则主动发送acknack
            /*      为什么发？
                StatefulReader 是可靠 reader。可靠通信里，writer 会发 HEARTBEAT 
                告诉 reader：“我这里有 seq 1 到 seq N”。
                reader 收到 heartbeat 后，再回 ACKNACK 告诉 writer：“哪些我收到了，哪些我缺”。
                但有个问题：如果 reader 刚匹配 writer 后，一直没收到 writer 的 heartbeat 怎么办？

                正常情况：
                writer heartbeat 到了
                -> reader 按 heartbeat 回复 ACKNACK
                -> initial ACKNACK 没必要

                异常/时序情况：
                reader 匹配 writer 后，没等到 heartbeat
                -> initial_acknack_ 定时器触发
                -> reader 主动发 initial ACKNACK
                -> writer 收到后可能回 heartbeat 或重发状态
            */
            ///@note reader 主动给 writer 发一个初始 ACKNACK，提醒 writer 发 heartbeat / 数据状态。
            // last_heartbeat_count_表示之前收到心跳的最后一个id，初始化值为0
            if (0 == last_heartbeat_count_)     // initial ACKNACK 是兜底机制：
            {
                reader_->send_acknack(this, sns, this, false);
                double time_ms = initial_acknack_->getIntervalMilliSec();
                constexpr double max_ms = 60 * 60 * 1000; // Limit to 1 hour
                if (time_ms < max_ms)
                {
                    initial_acknack_->update_interval_millisec(time_ms * 2);
                    ret_value = true;
                }
            }
        }
    }

    expected = StateCode::BUSY;
    state_.compare_exchange_strong(expected, StateCode::IDLE);

    return ret_value;
}

void WriterProxy::perform_heartbeat_response()
{
    StateCode expected = StateCode::IDLE;
    if (!state_.compare_exchange_strong(expected, StateCode::BUSY))
    {
        // Stopped from another thread -> abort
        return;
    }

    reader_->send_acknack(this, this, heartbeat_final_flag_.load());

    expected = StateCode::BUSY;
    state_.compare_exchange_strong(expected, StateCode::IDLE);
}

bool WriterProxy::process_heartbeat(
        uint32_t count,
        const SequenceNumber_t& first_seq,
        const SequenceNumber_t& last_seq,
        bool final_flag,
        bool liveliness_flag,
        bool disable_positive,
        bool& assert_liveliness,
        int32_t& current_sample_lost)
{
#ifdef SHOULD_DEBUG_LINUX
    assert(get_mutex_owner() == get_thread_id());
#endif // SHOULD_DEBUG_LINUX

    assert_liveliness = false;
    if (state_ != StateCode::STOPPED && last_heartbeat_count_ < count)
    {
        // If it is the first heartbeat message, we can try to cancel initial ack.
        // TODO: This timer cancelling should be checked if needed with the liveliness implementation.
        // To keep PARTICIPANT_DROPPED event we should add an explicit participant_liveliness QoS.
        // This is now commented to avoid issues #457 and #155
        // initial_acknack_->cancel_timer();

        //last_heartbeat_count_ 收到的最新心跳的数量
        last_heartbeat_count_ = count;
        // 看了一下，缺了几个message
        current_sample_lost = lost_changes_update(first_seq);
        // 更新一下writer messageid的上限
        missing_changes_update(last_seq);
        //final flag 如果心跳中有final flag，reader 不一定需要发送acknack消息，如果没有final flag reader一定要发送ackanck消息
        heartbeat_final_flag_.store(final_flag);

        //Analyze whether a acknack message is needed:
        
        if (!is_on_same_process_)
        {
            // NACK: 我缺这些序号，请重传
            // ACK:  我不缺，告诉你我都收到了

            // final_flag 决定“没缺数据时是否可以不回”
            // disable_positive 决定“是否允许纯 ACK”
            // liveliness_flag 决定“这是不是只用来保活”

            // 缺数据时，reader 通常要发 ACKNACK 请求重传。
            // 不缺数据时，只有在 !final_flag 且没有 disable_positive 的情况下，才会发纯 ACK。
            // liveliness-only heartbeat 只刷新活性，不走 ACKNACK。

            // 不是 final heartbeat。
            // 如果允许 positive ACK，回 ACKNACK。
            // 如果禁用了 positive ACK，那只有缺数据才回 ACKNACK。
            if (!final_flag)   // reader 比较积极地回应。
            {
                // !disable_positive  表示允许正向确认。那即使不缺数据，也可以发 ACKNACK，当作纯 ACK
                if (!disable_positive || are_there_missing_changes())
                {
                    heartbeat_response_->restart_timer();
                }
            }
            // 一个 final heartbeat，如果 reader 没有缺数据，可以不用回复。 而且不是单纯保活 heartbeat。 
            else if (final_flag && !liveliness_flag)
            {
                // 如果缺数据
                if (are_there_missing_changes())
                {
                    heartbeat_response_->restart_timer();
                }
            }
            else  // final_flag == true     liveliness_flag == true 
            {
                // 收到保活 heartbeat 了，确认 writer 活着即可，不需要请求重传，也不需要纯 ACK。
                assert_liveliness = liveliness_flag;
            }
        }
        else
        {
            assert_liveliness = liveliness_flag;
        }

        if (!received_at_least_one_heartbeat_)
        {
            current_sample_lost = 0;
            received_at_least_one_heartbeat_ = true;
        }

        return true;
    }

    return false;
}

void WriterProxy::update_heartbeat_response_interval(
        const dds::Duration_t& interval)
{
    heartbeat_response_->update_interval(interval);
}

bool WriterProxy::send(
        const std::vector<eprosima::fastdds::rtps::NetworkBuffer>& buffers,
        const uint32_t& total_bytes,
        std::chrono::steady_clock::time_point max_blocking_time_point)
{
    if (is_on_same_process_)
    {
        return true;
    }

    const ResourceLimitedVector<Locator_t>& remote_locators = remote_locators_shrinked();

    return reader_->send_sync_nts(buffers,
                   total_bytes,
                   Locators(remote_locators.begin()),
                   Locators(remote_locators.end()),
                   max_blocking_time_point);
}

#ifdef SHOULD_DEBUG_LINUX
int WriterProxy::get_mutex_owner() const
{
    auto mutex = reader_->getMutex().native_handle();
    return mutex->__data.__owner;
}

int WriterProxy::get_thread_id() const
{
    return syscall(__NR_gettid);
}

#endif // SHOULD_DEBUG_LINUX

} /* namespace rtps */
} /* namespace fastdds */
} /* namespace eprosima */
