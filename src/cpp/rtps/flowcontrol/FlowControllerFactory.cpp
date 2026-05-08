#include "FlowControllerFactory.hpp"
#include "FlowControllerImpl.hpp"

#include <fastdds/dds/log/Log.hpp>

namespace eprosima {
namespace fastdds {
namespace rtps {

void FlowControllerFactory::init(
        fastdds::rtps::RTPSParticipantImpl* participant)
{
    participant_ = participant;
    // Create default flow controllers.

    const ThreadSettings& sender_thread_settings =
            (nullptr == participant_) ? ThreadSettings{}
            : participant_->get_attributes().builtin_controllers_sender_thread;
    // 纯同步的，先进先出（fifo）的发送模式，纯同步就是完全没有异步发送方式
    // PureSyncFlowController -> used by volatile besteffort writers.
    flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                pure_sync_flow_controller_name,
                std::unique_ptr<FlowController>(
                    new FlowControllerImpl<FlowControllerPureSyncPublishMode,
                    FlowControllerFifoSchedule>(participant_, nullptr, 0, sender_thread_settings))));
    // 同步的，先进先出（fifo）的发送模式，如果同步发送不成功，则异步发送         
    // SyncFlowController -> used by rest of besteffort writers.
    flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                sync_flow_controller_name,
                std::unique_ptr<FlowController>(
                    new FlowControllerImpl<FlowControllerSyncPublishMode,
                    FlowControllerFifoSchedule>(participant_, nullptr, async_controller_index_++,
                    sender_thread_settings))));
    // 异步的，先进先出（fifo）的发送模式，异步的就是没有同步发送的方式，都是异步发送
    // AsyncFlowController
    flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                async_flow_controller_name,
                std::unique_ptr<FlowController>(
                    new FlowControllerImpl<FlowControllerAsyncPublishMode,
                    FlowControllerFifoSchedule>(participant_, nullptr, async_controller_index_++,
                    sender_thread_settings))));

#ifdef FASTDDS_STATISTICS
    flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                async_statistics_flow_controller_name,
                std::unique_ptr<FlowController>(
                    new FlowControllerImpl<FlowControllerAsyncPublishMode,
                    FlowControllerFifoSchedule>(participant_, nullptr, async_controller_index_++,
                    sender_thread_settings))));
#endif // ifndef FASTDDS_STATISTICS
}

void FlowControllerFactory::register_flow_controller (
        const FlowControllerDescriptor& flow_controller_descr)
{
    if (flow_controllers_.end() != flow_controllers_.find(flow_controller_descr.name))
    {
        EPROSIMA_LOG_ERROR(RTPS_PARTICIPANT,
                "Error registering FlowController " << flow_controller_descr.name << ". Already registered");
        return;
    }

    const ThreadSettings& sender_thread_settings = flow_controller_descr.sender_thread;
    // 如果设置流量控制
    if (0 < flow_controller_descr.max_bytes_per_period)
    {
        switch (flow_controller_descr.scheduler)
        {
            case FlowControllerSchedulerPolicy::FIFO:
                flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                            flow_controller_descr.name,
                            std::unique_ptr<FlowController>(
                                new FlowControllerImpl<FlowControllerLimitedAsyncPublishMode,
                                FlowControllerFifoSchedule>(participant_,
                                &flow_controller_descr, async_controller_index_++, sender_thread_settings))));
                break;
            case FlowControllerSchedulerPolicy::ROUND_ROBIN:
                flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                            flow_controller_descr.name,
                            std::unique_ptr<FlowController>(
                                new FlowControllerImpl<FlowControllerLimitedAsyncPublishMode,
                                FlowControllerRoundRobinSchedule>(participant_,
                                &flow_controller_descr, async_controller_index_++, sender_thread_settings))));
                break;
            case FlowControllerSchedulerPolicy::HIGH_PRIORITY:
                flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                            flow_controller_descr.name,
                            std::unique_ptr<FlowController>(
                                new FlowControllerImpl<FlowControllerLimitedAsyncPublishMode,
                                FlowControllerHighPrioritySchedule>(participant_,
                                &flow_controller_descr, async_controller_index_++, sender_thread_settings))));
                break;
            case FlowControllerSchedulerPolicy::PRIORITY_WITH_RESERVATION:
                flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                            flow_controller_descr.name,
                            std::unique_ptr<FlowController>(
                                new FlowControllerImpl<FlowControllerLimitedAsyncPublishMode,
                                FlowControllerPriorityWithReservationSchedule>(participant_,
                                &flow_controller_descr, async_controller_index_++, sender_thread_settings))));
                break;
            default:
                assert(false);
        }
    }
    // 没有设置流量控制
    else
    {
        switch (flow_controller_descr.scheduler)
        {
            case FlowControllerSchedulerPolicy::FIFO:
                flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                            flow_controller_descr.name,
                            std::unique_ptr<FlowController>(
                                new FlowControllerImpl<FlowControllerAsyncPublishMode,
                                FlowControllerFifoSchedule>(participant_,
                                &flow_controller_descr, async_controller_index_++, sender_thread_settings))));
                break;
            case FlowControllerSchedulerPolicy::ROUND_ROBIN:
                flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                            flow_controller_descr.name,
                            std::unique_ptr<FlowController>(
                                new FlowControllerImpl<FlowControllerAsyncPublishMode,
                                FlowControllerRoundRobinSchedule>(participant_,
                                &flow_controller_descr, async_controller_index_++, sender_thread_settings))));
                break;
            case FlowControllerSchedulerPolicy::HIGH_PRIORITY:
                flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                            flow_controller_descr.name,
                            std::unique_ptr<FlowController>(
                                new FlowControllerImpl<FlowControllerAsyncPublishMode,
                                FlowControllerHighPrioritySchedule>(participant_,
                                &flow_controller_descr, async_controller_index_++, sender_thread_settings))));
                break;
            case FlowControllerSchedulerPolicy::PRIORITY_WITH_RESERVATION:
                flow_controllers_.insert(decltype(flow_controllers_)::value_type(
                            flow_controller_descr.name,
                            std::unique_ptr<FlowController>(
                                new FlowControllerImpl<FlowControllerAsyncPublishMode,
                                FlowControllerPriorityWithReservationSchedule>(participant_,
                                &flow_controller_descr, async_controller_index_++, sender_thread_settings))));
                break;
            default:
                assert(false);
        }
    }
}

/*!
 * Get a FlowController given its name.
 *
 * @param flow_controller_name Name of the interested FlowController.
 * @return Pointer to the FlowController. nullptr if no registered FlowController with that name.
 */
FlowController* FlowControllerFactory::retrieve_flow_controller(
        const std::string& flow_controller_name,
        const fastdds::rtps::WriterAttributes& writer_attributes)
{
    FlowController* returned_flow = nullptr;

    // Detect it has to be returned a default flow_controller.
    // 默认的flowcontroller
    if (0 == flow_controller_name.compare(FASTDDS_FLOW_CONTROLLER_DEFAULT))
    {
        //如果是同步的writer
        if (fastdds::rtps::SYNCHRONOUS_WRITER == writer_attributes.mode)
        {
            //如果说BEST_EFFORT  BEST_EFFORT 语义上不要求确认、重传、补历史。 
            if (fastdds::rtps::BEST_EFFORT == writer_attributes.endpoint.reliabilityKind)
            {
                // pure_sync_flow_controller 只同步发新数据，没有 async thread，没有 old sample 重发。
                returned_flow = flow_controllers_[pure_sync_flow_controller_name].get();
            }
            else
            {
                // sync_flow_controller_name  新数据先同步发，但保留异步补发/旧样本重发能力。
                returned_flow = flow_controllers_[sync_flow_controller_name].get();
            }
        }
        else
        {
            // 新数据直接入队异步发送。
            returned_flow = flow_controllers_[async_flow_controller_name].get();
        }
    }
#ifdef FASTDDS_STATISTICS
    else if (0 == flow_controller_name.compare(FASTDDS_STATISTICS_FLOW_CONTROLLER_DEFAULT))
    {
        assert(fastdds::rtps::ASYNCHRONOUS_WRITER == writer_attributes.mode);
        returned_flow = flow_controllers_[async_statistics_flow_controller_name].get();
    }
#endif // ifdef FASTDDS_STATISTICS
    else
    {
        auto it = flow_controllers_.find(flow_controller_name);

        if (flow_controllers_.end() != it)
        {
            returned_flow = it->second.get();
        }
    }

    if (nullptr != returned_flow)
    {
        returned_flow->init();
    }
    else
    {
        EPROSIMA_LOG_ERROR(RTPS_PARTICIPANT,
                "Cannot find FlowController " << flow_controller_name << ".");
    }

    return returned_flow;
}

} // namespace rtps
} // namespace fastdds
} // namespace eprosima
