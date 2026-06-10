#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

#include "node.hpp"

int main(int argc, char* argv[])
{
    bool crashTestEnabled = false;
    for (int i = 1; i < argc; ++i)
    {
        if (std::string(argv[i]) == "--crash-test")
            crashTestEnabled = true;
    }

    const int CLUSTER_SIZE = 5;

    std::vector<std::unique_ptr<Node>> nodes;
    for (int i = 0; i < CLUSTER_SIZE; ++i)
        nodes.push_back(std::make_unique<Node>(i)); //not starting it yet, just initializing (I need to set the cluster before starting)

    std::vector<Node*> cluster;
    for (auto& n : nodes) cluster.push_back(n.get());
    for (auto& n : nodes) n->setCluster(cluster);
    for (auto& n : nodes) n->start();

    std::cout << "Cluster started, waiting for leader election\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    /*In the optimized raft algorithm, the client randomly pings a node and if the node accepts the message if it is indded the real leader else it redirects the client
      to the actual leader. Here, to simplify the code, I ping all the nodes until one leader acknowledges that it accepted and forwarded the update. */
    std::cout << "\nSubmitting commands\n";
    const std::vector<std::string> commands = {"SET x=1", "SET y=2", "SET z=3"};
    const int CRASH_TEST_COMMAND_ID = 0;
    const int CRASH_AFTER_FOLLOWERS = 2;
    int crashedLeaderId = -1;

    for (int commandId = 0; commandId < (int)commands.size(); ++commandId)
    {
        const auto& cmd = commands[commandId];
        bool acknowledged = false;

        if (crashTestEnabled && commandId == CRASH_TEST_COMMAND_ID && crashedLeaderId == -1)
        {
            std::cout << "\n[Main] Crash test: leader will forward command #"
                      << commandId << " to " << CRASH_AFTER_FOLLOWERS
                      << " follower(s), then die before ack\n";

            while (crashedLeaderId == -1)
            {
                for (auto& n : nodes)
                {
                    if (n->submitCommandAndCrashBeforeAck(commandId, cmd, CRASH_AFTER_FOLLOWERS))
                    {
                        crashedLeaderId = n->getId();
                        break;
                    }
                }

                if (crashedLeaderId == -1)
                {
                    std::cout << "[Main] No leader found for crash test yet, retrying\n";
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }

            std::cout << "[Main] Leader " << crashedLeaderId
                      << " died before ack; retrying command #" << commandId
                      << " until a new leader accepts it\n";
        }

        while (!acknowledged)
        {
            for (auto& n : nodes)
            {
                if (n->submitCommand(commandId, cmd))
                {
                    acknowledged = true;
                    break;
                }
            }

            if (!acknowledged)
            {
                std::cout << "[Main] No leader acknowledged command #" << commandId
                          << ", retrying\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        std::cout << "[Main] Command #" << commandId << " acknowledged by leader\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200)); //buffer for the leader to commit
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    if (crashedLeaderId != -1)
    {
        std::cout << "\n[Main] Restarting crashed node " << crashedLeaderId
                  << " so it can catch up from the current leader\n";
        nodes[crashedLeaderId]->start();
    }

    bool allApplied = false;
    for (int attempt = 0; attempt < 20 && !allApplied; ++attempt)
    {
        allApplied = true;
        for (auto& n : nodes)
        {
            if (!n->isRunning())
            {
                allApplied = false;
                break;
            }

            for (int commandId = 0; commandId < (int)commands.size(); ++commandId)
            {
                if (!n->hasAppliedCommand(commandId))
                {
                    allApplied = false;
                    break;
                }
            }

            if (!allApplied) break;
        }

        if (!allApplied)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "\n[Main] Final command application check\n";
    for (auto& n : nodes)
    {
        bool nodeAppliedAll = n->isRunning();
        for (int commandId = 0; commandId < (int)commands.size(); ++commandId)
            nodeAppliedAll = nodeAppliedAll && n->hasAppliedCommand(commandId);

        std::cout << "[Main] Node " << n->getId() << ": "
                  << (nodeAppliedAll ? "applied all commands" : "missing commands")
                  << "\n";
    }

    std::cout << "[Main] Cluster result: "
              << (allApplied ? "all nodes caught up" : "some nodes are still missing commands")
              << "\n";

    std::cout << "\n Stopping cluster \n";
    for (auto& n : nodes) n->stop();

    return 0;
}
