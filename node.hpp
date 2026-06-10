#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "message.hpp"
#include "logEntry.hpp"

enum class NodeState
{
    FOLLOWER,
    CANDIDATE,
    LEADER
};

class Node
{
public:
    explicit Node(int id);
    ~Node();

    void start();
    void stop();
    void enqueue(std::unique_ptr<Message> msg);
    void setCluster(const std::vector<Node*>& nodes);
    bool submitCommand(int commandId, const std::string& command); //the client will call this indirectly 
    bool submitCommandAndCrashBeforeAck(int commandId, const std::string& command, int followersToReach);
    bool hasAppliedCommand(int commandId);

    int getId() const;
    bool isRunning() const;

private:

    int id;

    NodeState state = NodeState::FOLLOWER;
    int currentTerm = 0;
    int votedFor = -1;

    std::vector<LogEntry> log;
    int commitIndex = -1;
    int lastApplied = -1;

    //if we were to implement raft as is, we would need nextIndex as well, which will store the index from which the leader should send entries to each of the nodes
    //but here we are just sending the the leader's full log to each of the nodes.
    std::unordered_map<int, int> matchIndex; //stores uptil what index the follower node is following the leader. It is a leader only state

    std::unordered_set<int> votesReceived;

    std::chrono::steady_clock::time_point lastHeartbeatReceived;
    std::chrono::steady_clock::time_point lastHeartbeatSent; //only used by the leader

    std::chrono::milliseconds electionTimeout;

    std::vector<Node*> cluster;
    std::queue<std::unique_ptr<Message>> inbox;
    
    std::thread worker;
    std::atomic<bool> running{false};

    /*
    Each node object will have 2 mutexes: inbox and state
    -Worker threads of nodes wishing to communicate to other ndoes will append a message in the inbox of that particular node. After which the worker thread of the messaged
    node will take the message from the inbox. So we need a mutex for the inbox that will prevent collision between worker threads of differnet nodes.
    -The logs of the node, the state variable of the node etc are also accessed by the main thread through the submitCommand function and therefore requires a mutex.
    So we need to provide some of the node members with the stateMutex.
    */

    std::mutex inboxMutex; 
    std::mutex stateMutex;

private:
    void run();

    void processMessages();
    void processMessage(std::unique_ptr<Message> msg);

    void becomeFollower(int term); //when a node becomes a follower, it is usually out of date, so its term has to be updated
    void becomeCandidate();
    void becomeLeader();

    void startElection();
    void sendVoteRequests();

    void sendHeartbeats();
    int replicateLogToFollowers(int maxFollowers);

    void replicateLog(int followerId);

    void handleVoteRequest(const VOTE_REQUEST& msg);
    void handleVoteResponse(const VOTE_RESPONSE& msg);
    void handleLogUpdate(const LOG_UPDATE& msg);
    void handleLogUpdateResponse(const LOG_UPDATE_RESPONSE& msg);

    bool hasCommand(int commandId) const;

    int lastLogIndex() const;
    int lastLogTerm() const;

    bool isLogUpToDate(
        int candidateLastIndex,
        int candidateLastTerm
    ) const;
};
