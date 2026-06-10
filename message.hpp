#pragma once
#include <vector>
#include "logEntry.hpp"
using namespace std;


enum class MessageType{
    LOG_UPDATE,
    LOG_UPDATE_RESPONSE,
    VOTE_REQUEST,
    VOTE_RESPONSE,

};

struct Message{
    MessageType type;
    int sender_id;
    int receiver_id;
    int term;
    Message(MessageType t):type(t){};
    virtual ~Message() = default;
};

struct LOG_UPDATE: Message{
    LOG_UPDATE():Message(MessageType::LOG_UPDATE){};
    vector<LogEntry>entries;
    int prev_log_index;
    int prev_log_term;
    int leader_commit;
    
};

struct LOG_UPDATE_RESPONSE: Message{
    bool success;
    int match_index;
    LOG_UPDATE_RESPONSE():Message(MessageType::LOG_UPDATE_RESPONSE){};
    
};

struct VOTE_REQUEST: Message{
    int last_log_index;
    int last_log_term;
    VOTE_REQUEST():Message(MessageType::VOTE_REQUEST){};
};

struct VOTE_RESPONSE: Message{
    bool vote_granted;
    VOTE_RESPONSE(bool granted):Message(MessageType::VOTE_RESPONSE), vote_granted(granted){};
};
