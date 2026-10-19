#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <algorithm>
#include "waitforgraph.h"
#include "deadlock_sim.h"  
#include "scheduling.h"
using namespace std;
bool simulation_running = true;

void deadlockDetectorDaemon(WFG& wfg, LockManager& lm) {
    while (simulation_running) {
        this_thread::sleep_for(chrono::milliseconds(500));
        
        vector<string> cycle = wfg.detectDeadlock();
        
        if (!cycle.empty()) {
            cout << "\n======================================================\n";
            cout << "  DEADLOCK DETECTED! Cycle: ";
            for (size_t i = 0; i < cycle.size(); i++) {
                cout << cycle[i] << (i < cycle.size() - 1 ? " -> " : "");
            }
            cout << "\n======================================================\n";
            
         
            string victim = cycle.back();
            lm.rollbackTransaction(victim);
        }
    }
}

int main() {
    cout << "\n";
    cout << "  AI-Based Deadlock Detection -- Backend Engine Started \n";
    cout << "\n\n";


    TelemetryLogger logger;
    WFG wfg;
    LockManager lm(logger, wfg);

    // Register Database Resources
    lm.registerResource("R1");
    lm.registerResource("R2");
    lm.registerResource("R3");

    // Define OS Workloads (Transactions)
    struct TxConfig { string id; int burst; int prio; vector<string> reqs; };
    vector<TxConfig> workloads = {
        {"T1", 2, 2, {"R1", "R2"}},
        {"T2", 3, 1, {"R2", "R3"}},
        {"T3", 1, 3, {"R3", "R1"}}  
    };

    sort(workloads.begin(), workloads.end(), [](TxConfig& a, TxConfig& b) {
        return a.prio < b.prio;
    });

    for(auto& w : workloads) {
        Transaction t{w.id, w.burst, w.prio};
        lm.registerTransaction(t);
    }

    cout << " Spawning Deadlock Detector Daemon...\n";
    thread detectorThread(deadlockDetectorDaemon, ref(wfg), ref(lm));

    cout << " Scheduling Concurrent OS Threads...\n\n";
    vector<thread> os_threads;
    for (const auto& w : workloads) {
        os_threads.emplace_back(simulateTransaction, ref(lm), w.id, w.reqs, w.burst, ref(simulation_running));
        this_thread::sleep_for(chrono::milliseconds(50)); 
    }

    for (auto& t : os_threads) {
        if(t.joinable()) t.join();
    }

    simulation_running = false;
    if(detectorThread.joinable()) detectorThread.join();
    logger.shutdown();

    cout << "\n DONE! Simulation finished. Telemetry safely written to 'integrated_deadlock.db'\n";
    return 0;
}