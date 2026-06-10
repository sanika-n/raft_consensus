#include "node.hpp"
#include <iostream>
#include <random>

std::chrono::milliseconds randomElectionTimeout()
{
    std::random_device rd;
    std::uniform_int_distribution<int> dist(150,300);
    return std::chrono::milliseconds(dist(rd)); //if the timeout wasn't randomized then all nodes will start an election simultaenously and vote for themselves
}

constexpr std::chrono::milliseconds HEARTBEAT_INTERVAL{50};

Node::Node(int id) : id(id)
{
    electionTimeout = randomElectionTimeout();//each node gets a differnt one
    lastHeartbeatReceived = std::chrono::steady_clock::now();
}

Node::~Node() { stop(); }


int Node::getId() const { return id; }
bool Node::isRunning() const { return running; }
void Node::setCluster(const std::vector<Node*>& nodes) { cluster = nodes; } //set externally in main

void Node::enqueue(std::unique_ptr<Message> msg) //using a ptr to prevent object slicing
{
    if (!running) return;

    std::lock_guard<std::mutex> lk(inboxMutex);
    inbox.push(std::move(msg)); //moving ownership cuz unique ptr
}

void Node::start() // nodes are started in main
{
    if (running) return;

    {
        std::lock_guard<std::mutex> lk(stateMutex);
        lastHeartbeatReceived = std::chrono::steady_clock::now();
        electionTimeout = randomElectionTimeout();
    }

    running = true;
    worker = std::thread(&Node::run, this);
}

void Node::stop()
{
    running = false;
    if (worker.joinable()) worker.join();
}

bool Node::submitCommand(int commandId, const std::string& command)
{
    std::lock_guard<std::mutex> lk(stateMutex);
    if (!running)
    {
        std::cout << "[Node " << id << "] Down, cannot accept command: " << command << "\n";
        return false;
    }

    if (state != NodeState::LEADER)
    {
        std::cout << "[Node " << id << "] Not leader, ignoring command: " << command << "\n";
        return false;
    }

    if (hasCommand(commandId))
    {
        std::cout << "[Node " << id << "] Leader already has command #" << commandId << ": " << command << "\n";
    }
    else
    {
        log.push_back({currentTerm, commandId, command}); 
        /*Here the client thread(main thread) is directly touching the log and checking the state of the node, but it is safe
        cuz we are using the same stateMutex used within Node::run() */
        std::cout << "[Node " << id << "] Leader appended command #" << commandId << ": " << command << " at index " << (int)log.size()-1 << "\n";
    }

    sendHeartbeats();
    lastHeartbeatSent = std::chrono::steady_clock::now();
    return true;
}

bool Node::submitCommandAndCrashBeforeAck(int commandId, const std::string& command, int followersToReach)
{
    bool shouldJoin = false;
    {
        std::lock_guard<std::mutex> lk(stateMutex);
        if (!running || state != NodeState::LEADER)
            return false;

        if (hasCommand(commandId))
        {
            std::cout << "[Node " << id << "] Leader already has command #" << commandId << ": " << command << "\n";
        }
        else
        {
            log.push_back({currentTerm, commandId, command});
            std::cout << "[Node " << id << "] Leader appended command #" << commandId << ": " << command << " at index " << (int)log.size()-1 << "\n";
        }

        int sent = replicateLogToFollowers(followersToReach);
        std::cout << "[Node " << id << "] Simulated crash after forwarding command #"
                  << commandId << " to " << sent << " follower(s), before client ack\n";

        state = NodeState::FOLLOWER;
        running = false;
        shouldJoin = true;
    }

    if (shouldJoin && worker.joinable())
        worker.join();

    return true;
}

bool Node::hasAppliedCommand(int commandId)
{
    std::lock_guard<std::mutex> lk(stateMutex);
    for (int i = 0; i <= lastApplied && i < (int)log.size(); ++i)
    {
        if (log[i].commandId == commandId)
            return true;
    }
    return false;
}


void Node::run()
{
    while (running)
    {
        processMessages();

        {
            std::lock_guard<std::mutex> lk(stateMutex);
            auto now = std::chrono::steady_clock::now();

            if (state == NodeState::LEADER)
            {
                if (now - lastHeartbeatSent >= HEARTBEAT_INTERVAL)
                {
                    sendHeartbeats();
                    lastHeartbeatSent = now;
                }
            }
            else
            {
                if (now - lastHeartbeatReceived >= electionTimeout)
                {
                    becomeCandidate();
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}


void Node::processMessages()
{
    // we will empty the inbox into a local variable so that we hold inboxMutex as briefly as possible
    std::queue<std::unique_ptr<Message>> batch;
    {
        std::lock_guard<std::mutex> lk(inboxMutex);
        std::swap(batch, inbox);
    }

    while (!batch.empty())
    {
        auto msg = std::move(batch.front());
        batch.pop();
        processMessage(std::move(msg));
    }
}

void Node::processMessage(std::unique_ptr<Message> msg)
{
    std::lock_guard<std::mutex> lk(stateMutex);

    // if we see a higher term, revert to follower immediately
    if (msg->term > currentTerm)
        becomeFollower(msg->term);

    switch (msg->type)
    {   /* whenever we call any of the functions mentioned in either of the cases, 
           the mutex will be held throughout the execution of the function as lk wont go out of scope  until the end of processMessage */

        case MessageType::VOTE_REQUEST:
            handleVoteRequest(static_cast<const VOTE_REQUEST&>(*msg)); //we don't need to use pointers anymore as the exact message type has been determined, so the "handle" functions can be provided with the direct object instead
            break;
        case MessageType::VOTE_RESPONSE:
            handleVoteResponse(static_cast<const VOTE_RESPONSE&>(*msg));
            break;
        case MessageType::LOG_UPDATE:
            handleLogUpdate(static_cast<const LOG_UPDATE&>(*msg));
            break;
        case MessageType::LOG_UPDATE_RESPONSE:
            handleLogUpdateResponse(static_cast<const LOG_UPDATE_RESPONSE&>(*msg));
            break;
    }
}

// Log helpers
int Node::lastLogIndex() const
{
    return (int)log.size() - 1;
}

int Node::lastLogTerm() const
{
    return log.empty() ? -1 : log.back().term;
}

bool Node::isLogUpToDate(int candidateLastIndex, int candidateLastTerm) const
{
    int myTerm  = lastLogTerm();
    int myIndex = lastLogIndex();
    if (candidateLastTerm != myTerm)
        return candidateLastTerm > myTerm; //candiadate should be ahead to return true
    return candidateLastIndex >= myIndex;
}

bool Node::hasCommand(int commandId) const
{
    for (const auto& entry : log)
    {
        if (entry.commandId == commandId)
            return true;
    }
    return false;
}

// The following functions must only be called while the stateMutex is held

void Node::becomeFollower(int term)
{
    std::cout << "[Node " << id << "] -> FOLLOWER (term " << term << ")\n";
    state = NodeState::FOLLOWER;
    currentTerm = term;
    votedFor = -1;
    votesReceived.clear(); //a node only receives vote in the candidate state, so calling it during becomeFollower ensures that stale votes are deleted from any prev states
    lastHeartbeatReceived = std::chrono::steady_clock::now();
    electionTimeout = randomElectionTimeout(); //to avoid repeating the same pattern that caused the split election this time
}

void Node::becomeCandidate()
{
    state = NodeState::CANDIDATE;
    ++currentTerm;
    votedFor = id;
    votesReceived.clear();
    votesReceived.insert(id); // vote for ourselves
    lastHeartbeatReceived = std::chrono::steady_clock::now();
    /* We need to reinitialize the electionTimeout any time a split election occurrs, in order to prevent the same pattern that caused the split election from happening again.
    When a candidate looses an election (split election) and he still doesn't receive a heartbeat, he remains a candidate and contests again unless its term goes out of date. So we need to reinitialize
    time out in becomeCandidate() */
    electionTimeout = randomElectionTimeout(); 
    std::cout << "[Node " << id << "] -> CANDIDATE (term " << currentTerm << ")\n";
    sendVoteRequests();
}

void Node::becomeLeader()
{
    
    state = NodeState::LEADER;
    std::cout << "[Node " << id << "] -> LEADER (term " << currentTerm << ")\n";

    for (auto* peer : cluster)
    {
        if (peer->getId() == id) continue;
        matchIndex[peer->getId()] = -1;
    }

    lastHeartbeatSent = std::chrono::steady_clock::now();
    sendHeartbeats();
}


void Node::sendVoteRequests()
{
    for (auto* peer : cluster)
    {
        if (peer->getId() == id) continue;

        auto req = std::make_unique<VOTE_REQUEST>();
        req->sender_id     = id;
        req->receiver_id   = peer->getId();
        req->term          = currentTerm;
        req->last_log_index = lastLogIndex();
        req->last_log_term  = lastLogTerm();

        peer->enqueue(std::move(req));
    }
}

void Node::sendHeartbeats()
{
    replicateLogToFollowers(-1);
}

int Node::replicateLogToFollowers(int maxFollowers)
{
    int sent = 0;
    for (auto* peer : cluster)
    {
        if (peer->getId() == id) continue;
        if (!peer->running) continue;

        replicateLog(peer->getId());
        ++sent;

        if (maxFollowers >= 0 && sent >= maxFollowers)
            break;
    }

    return sent;
}

void Node::replicateLog(int followerId)
{
    //Here since the length of logs are small I am sending the leader's complete log to the other nodes each time, in Raft this procedure is highly optimized and only missing entries are sent
    auto msg = std::make_unique<LOG_UPDATE>();
    msg->sender_id    = id;
    msg->receiver_id  = followerId;
    msg->term         = currentTerm;
    msg->entries      = log;
    msg->prev_log_index = -1; 
    msg->prev_log_term  = -1; 
    msg->leader_commit  = commitIndex;

    for (auto* peer : cluster){
        if (peer->getId() == followerId){
            peer->enqueue(std::move(msg));
            break;
        }
    }
}


void Node::handleVoteRequest(const VOTE_REQUEST& msg)
{
    bool grant = false;

    if (msg.term >= currentTerm && (votedFor == -1 || votedFor == msg.sender_id) &&
        isLogUpToDate(msg.last_log_index, msg.last_log_term))
    {
        grant    = true;
        votedFor = msg.sender_id;
        lastHeartbeatReceived = std::chrono::steady_clock::now(); //cuz this node thinks that msg.sender_id is legimate and is ok with electing them, so it will wait for the election result of that node before starting its own election
    }

    std::cout << "[Node " << id << "] Vote request from " << msg.sender_id<< " -> " << (grant ? "granted" : "denied") << "\n";

    auto resp = std::make_unique<VOTE_RESPONSE>(grant);
    resp->sender_id   = id;
    resp->receiver_id = msg.sender_id;
    resp->term        = currentTerm;

    for (auto* peer : cluster)
        if (peer->getId() == msg.sender_id)
            peer->enqueue(std::move(resp));
}

void Node::handleVoteResponse(const VOTE_RESPONSE& msg)
{
    if (state != NodeState::CANDIDATE)return;
    if (msg.term != currentTerm)return;

    if (msg.vote_granted)
    {
        votesReceived.insert(msg.sender_id);
        int majority = (int)cluster.size() / 2 + 1;
        std::cout << "[Node " << id << "] Vote from " << msg.sender_id
                  << " (" << votesReceived.size() << "/" << majority << " needed)\n";
        if ((int)votesReceived.size() >= majority)
            becomeLeader();
    }
}

void Node::handleLogUpdate(const LOG_UPDATE& msg)
{

    if (msg.term < currentTerm)
    {
        auto resp = std::make_unique<LOG_UPDATE_RESPONSE>();
        resp->sender_id   = id;
        resp->receiver_id = msg.sender_id;
        resp->term        = currentTerm;
        resp->success     = false;
        resp->match_index = -1;
        for (auto* peer : cluster)
            if (peer->getId() == msg.sender_id)
                peer->enqueue(std::move(resp));
        return;
    }

    lastHeartbeatReceived = std::chrono::steady_clock::now();
    if (state != NodeState::FOLLOWER)
        becomeFollower(msg.term);

    log = msg.entries;

    //each node has a commitIndex that is advanced according to the leader's commit index
    if (msg.leader_commit > commitIndex)
    {
        commitIndex = std::min(msg.leader_commit, (int)log.size() - 1); //the commit index of a node can not be ahead of the leader's commit index, so that only globally committable nodes are applied to the state machine

        // apply committed entries
        while (lastApplied < commitIndex)
        {
            ++lastApplied;
            std::cout << "[Node " << id << "] Applied: " << log[lastApplied].command<< " (index=" << lastApplied << ")\n";
        }
    }

    auto resp = std::make_unique<LOG_UPDATE_RESPONSE>();
    resp->sender_id   = id;
    resp->receiver_id = msg.sender_id;
    resp->term        = currentTerm;
    resp->success     = true;
    resp->match_index = (int)log.size() - 1;

    for (auto* peer : cluster)
        if (peer->getId() == msg.sender_id)
            peer->enqueue(std::move(resp));
}

void Node::handleLogUpdateResponse(const LOG_UPDATE_RESPONSE& msg)
{
    if (state != NodeState::LEADER) return;

    if (!msg.success) return; // stale / rejected, this means that the leader is not up to date anymore(likely due to a re-election), hence it would soon become a follower and stop resending failed messages.
    //ie in this implementation where we are fully replicating the log as is, falirure message is not necessary per se 

    matchIndex[msg.sender_id] = msg.match_index;


    for (int n = (int)log.size() - 1; n > commitIndex; --n)
    {
        int count = 1; // leader itself
        for (auto* peer : cluster)
        {
            if (peer->getId() == id) continue;
            if (matchIndex[peer->getId()] >= n) ++count;
        }

        int majority = (int)cluster.size() / 2 + 1;
        if (count >= majority)
        {
            commitIndex = n;
            //applying is pushing the changes to the final state machine
            //we are considering printing to be the equivalent of applying
            //anything that is committed (appears in a majority of the nodes) is safe to be applied
            while (lastApplied < commitIndex)
            {
                ++lastApplied;
                std::cout << "[Node " << id << "] Applied: " << log[lastApplied].command
                          << " (index=" << lastApplied << ")\n";
            }
            break;
        }
    }
}
