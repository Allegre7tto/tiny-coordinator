#pragma once

#include "common/types.h"

#include <string>
#include <utility>

namespace engine {

enum class StatusCode : int {
    kOk             = 0,
    kNotLeader      = 1,
    kNotFound       = 2,
    kAlreadyExists  = 3,
    kInvalidArg     = 4,
    kIOError        = 5,
    kTimeout        = 6,
    kInternal       = 7,
    kLeaseExpired   = 8,
    kCompacted      = 9,
};

class Status {
public:
    Status() = default;

    static Status OK()                            { return Status{StatusCode::kOk, ""}; }
    static Status NotLeader(uint64 leader_hint = 0) {
        Status s{StatusCode::kNotLeader, "not leader"};
        s.leader_hint_ = leader_hint;
        return s;
    }
    static Status NotFound(std::string msg = "not found")   { return {StatusCode::kNotFound,     std::move(msg)}; }
    static Status InvalidArg(std::string msg)               { return {StatusCode::kInvalidArg,   std::move(msg)}; }
    static Status IOError(std::string msg)                  { return {StatusCode::kIOError,       std::move(msg)}; }
    static Status Timeout(std::string msg = "timeout")      { return {StatusCode::kTimeout,       std::move(msg)}; }
    static Status Internal(std::string msg)                 { return {StatusCode::kInternal,      std::move(msg)}; }
    static Status LeaseExpired()                            { return {StatusCode::kLeaseExpired,  "lease expired"}; }

    bool ok()         const { return code_ == StatusCode::kOk; }
    bool IsNotLeader() const { return code_ == StatusCode::kNotLeader; }
    bool IsNotFound()  const { return code_ == StatusCode::kNotFound; }
    bool IsTimeout()   const { return code_ == StatusCode::kTimeout; }

    StatusCode code()           const { return code_; }
    const std::string& message() const { return msg_; }
    uint64 leader_hint()         const { return leader_hint_; }

    std::string ToString() const {
        if (ok()) return "OK";
        return "Status(" + std::to_string(static_cast<int>(code_)) + "): " + msg_;
    }

private:
    Status(StatusCode c, std::string m) : code_(c), msg_(std::move(m)) {}

    StatusCode  code_        = StatusCode::kOk;
    std::string msg_;
    uint64      leader_hint_ = 0;
};

} // namespace engine
