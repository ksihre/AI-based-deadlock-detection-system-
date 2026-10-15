#include <iostream>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <algorithm>

using namespace std;

class WFG {
private:
    unordered_map<int, vector<int>> adj;
    mutex m; 

    bool checkCycle(int node, unordered_map<int, bool>& vis, unordered_map<int, bool>& pathVis, vector<int>& path) {
        vis[node] = true;
        pathVis[node] = true;
        path.push_back(node);

        for (int it : adj[node]) {
            if (!vis[it]) {
                if (checkCycle(it, vis, pathVis, path)) return true;
            } 
            else if (pathVis[it]) {
                path.push_back(it); 
                return true;
            }
        }

        pathVis[node] = false;
        path.pop_back();
        return false;
    }

public:
    void addWaitEdge(int waitingTx, int holdingTx) {
        lock_guard<mutex> lock(m);
        // Add edge only if it doesn't already exist
        if(find(adj[waitingTx].begin(), adj[waitingTx].end(), holdingTx) == adj[waitingTx].end()) {
            adj[waitingTx].push_back(holdingTx);
        }
    }

    void removeTx(int txId) {
        lock_guard<mutex> lock(m);
        adj.erase(txId); 
        
        for (auto& pair : adj) {
            auto it = find(pair.second.begin(), pair.second.end(), txId);
            if (it != pair.second.end()) {
                pair.second.erase(it);
            }
        }
    }

    vector<int> detectDeadlock() {
        unordered_map<int, vector<int>> graphCopy;
        {
            // Snapshot of the graph for thread safety
            lock_guard<mutex> lock(m);
            graphCopy = adj;
        }

        unordered_map<int, bool> vis;
        unordered_map<int, bool> pathVis;
        vector<int> path;

        for (auto pair : graphCopy) {
            if (!vis[pair.first]) {
                if (checkCycle(pair.first, vis, pathVis, path)) {
                    auto start = find(path.begin(), path.end(), path.back());
                    return vector<int>(start, path.end());
                }
            }
        }
        return {}; 
    }
};

int main() {
    WFG graph;
    int choice;

   
    cout << "   Wait-For Graph (WFG) Live Simulation   \n";
   

    while(true) {
        cout << "\nMenu Options:\n";
        cout << "1. Add Lock Request (Create Wait Edge)\n";
        cout << "2. Check System for Deadlocks\n";
        cout << "3. Rollback Transaction (Victim Selection)\n";
        cout << "4. Exit\n";
        cout << "Enter your choice: ";
        cin >> choice;

        if (choice == 1) {
            int waiting, holding;
            cout << "Enter waiting Transaction ID: ";
            cin >> waiting;
            cout << "Enter holding Transaction ID: ";
            cin >> holding;
            
            graph.addWaitEdge(waiting, holding);
            cout << "-> Successfully mapped: T" << waiting << " is waiting for T" << holding << "\n";

        } else if (choice == 2) {
            vector<int> ans = graph.detectDeadlock();
            if (ans.empty()) {
                cout << "-> System is SAFE. No deadlocks detected right now.\n";
            } else {
                cout << "-> [CRITICAL] DEADLOCK DETECTED! Cycle: ";
                for(int i = 0; i < ans.size(); i++) {
                    cout << "T" << ans[i] << (i < ans.size() - 1 ? " -> " : "");
                }
                cout << "\n";
            }

        } else if (choice == 3) {
            int victim;
            cout << "Enter Transaction ID to rollback: ";
            cin >> victim;
            
            graph.removeTx(victim);
            cout << "-> Transaction T" << victim << " has been rolled back and resources cleared.\n";

        } else if (choice == 4) {
            cout << "Exiting simulation...\n";
            break;
            
        } else {
            cout << "Invalid input. Please enter a number between 1 and 4.\n";
        }
    }

    return 0;
}