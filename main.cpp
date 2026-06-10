#include <iostream>
#include <memory>
#include <vector>
#include <thread>
#include <chrono>

#include "node.hpp"

int main()
{
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
      to the actual leader. Here, to simplify the code, I just ping all the nodes with the update and the leader accepts the update. */
    std::cout << "\nSubmitting commands\n";
    const std::vector<std::string> commands = {"SET x=1", "SET y=2", "SET z=3"};
    for (const auto& cmd : commands)
    {
        for (auto& n : nodes)
            n->submitCommand(cmd);
        std::this_thread::sleep_for(std::chrono::milliseconds(200)); //buffer for the leader to commit
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    std::cout << "\n Stopping cluster \n";
    for (auto& n : nodes) n->stop();

    return 0;
}

