/*
 * +======================================================================+
 * |  AI-Based Deadlock Detection -- DBMS & Lock Management Prototype     |
 * |  ----------------------------------------------------------------    |
 * |  A multithreaded simulation where concurrent database transactions   |
 * |  (modelled as OS threads) compete for shared resources.              |
 * |                                                                      |
 * |  Requirements implemented:                                           |
 * |    1. In-memory data structures  (Transaction, Resource, LockTable)  |
 * |    2. Lock acquisition / release / rollback logic                    |
 * |    3. Thread synchronization via std::mutex                          |
 * |    4. SQLite database schema (Transactions + Telemetry_Log)          |
 * |    5. Thread-safe database logging (async queue + worker thread)     |
 * +======================================================================+
 *
 *  Compile (macOS / Linux):
 *    g++ -std=c++17 -pthread -o deadlock_sim deadlock_sim.cpp -lsqlite3
 *
 *  Run:
 *    ./deadlock_sim
 */

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <sqlite3.h>   // link with -lsqlite3

// ======================================================================
// 1. IN-MEMORY DATA STRUCTURES
// ======================================================================

/** Possible lifecycle states for a transaction. */
enum class TxnStatus {
    RUNNING,
    WAITING,
    ABORTED,
    COMMITTED
};

/** Map TxnStatus to its printable string (matches the Python values). */
static const char* txnStatusToStr(TxnStatus s) {
    switch (s) {
        case TxnStatus::RUNNING:   return "Running";
        case TxnStatus::WAITING:   return "Waiting";
        case TxnStatus::ABORTED:   return "Aborted";
        case TxnStatus::COMMITTED: return "Committed";
    }
    return "Unknown";
}

/**
 * Represents a single database transaction.
 *
 *  txn_id          – Unique identifier for the transaction.
 *  priority        – Priority level (lower number = higher priority).
 *  status          – Current lifecycle status.
 *  held_resources  – List of Resource IDs currently held.
 *  waiting_for     – The Resource ID this transaction is blocking on (if any).
 */
struct Transaction {
    std::string              txn_id;
    int                      priority;
    TxnStatus                status          = TxnStatus::RUNNING;
    std::vector<std::string> held_resources;
    std::string              waiting_for;      // empty string == not waiting

    std::string repr() const {
        std::ostringstream oss;
        oss << "Transaction(id=" << txn_id
            << ", priority=" << priority
            << ", status=" << txnStatusToStr(status)
            << ", holds=[";
        for (size_t i = 0; i < held_resources.size(); ++i) {
            if (i) oss << ", ";
            oss << held_resources[i];
        }
        oss << "], waiting_for="
            << (waiting_for.empty() ? "None" : waiting_for)
            << ")";
        return oss.str();
    }
};

/**
 * Represents a shared resource (e.g., a DB row/page lock).
 *
 *  resource_id   – Unique identifier for the resource.
 *  owner_txn_id  – Transaction ID of the current owner, or "" if free.
 */
struct Resource {
    std::string resource_id;
    std::string owner_txn_id;   // empty == FREE

    bool isFree() const { return owner_txn_id.empty(); }

    std::string repr() const {
        std::string owner = owner_txn_id.empty() ? "FREE" : owner_txn_id;
        return "Resource(id=" + resource_id + ", owner=" + owner + ")";
    }
};


// ======================================================================
// 4. DATABASE SCHEMA INTEGRATION  (SQLite for prototyping)
// ======================================================================

static const char* DB_FILE = "deadlock_sim.db";

static const char* SCHEMA_SQL = R"SQL(
-- Stores the final outcome of every transaction that participated in the sim.
CREATE TABLE IF NOT EXISTS Transactions (
    Transaction_ID  TEXT PRIMARY KEY,
    Priority        INTEGER NOT NULL,
    Final_Status    TEXT    NOT NULL DEFAULT 'Running'
);

-- Append-only event log for every lock-related action.
CREATE TABLE IF NOT EXISTS Telemetry_Log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    Timestamp       TEXT    NOT NULL,
    Transaction_ID  TEXT    NOT NULL,
    Action          TEXT    NOT NULL CHECK (Action IN ('Acquire', 'Wait', 'Release', 'Rollback')),
    Resource_ID     TEXT    NOT NULL,
    FOREIGN KEY (Transaction_ID) REFERENCES Transactions(Transaction_ID)
);
)SQL";


/** Create (or recreate) the SQLite database with the required schema. */
static void initDatabase(const char* dbPath = DB_FILE) {
    // Remove stale DB from a previous run so results are clean.
    std::remove(dbPath);

    sqlite3* db = nullptr;
    if (sqlite3_open(dbPath, &db) != SQLITE_OK) {
        std::cerr << "[DB] Failed to open database: " << sqlite3_errmsg(db) << "\n";
        sqlite3_close(db);
        std::exit(1);
    }
    char* errMsg = nullptr;
    if (sqlite3_exec(db, SCHEMA_SQL, nullptr, nullptr, &errMsg) != SQLITE_OK) {
        std::cerr << "[DB] Schema error: " << errMsg << "\n";
        sqlite3_free(errMsg);
        sqlite3_close(db);
        std::exit(1);
    }
    sqlite3_close(db);
    std::cout << "[DB] Initialized database: " << dbPath << "\n";
}


// ======================================================================
// Helper: ISO-8601 timestamp with milliseconds
// ======================================================================

static std::string nowISO() {
    using namespace std::chrono;
    auto now   = system_clock::now();
    auto ms    = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    auto timer = system_clock::to_time_t(now);
    std::tm bt{};
    localtime_r(&timer, &bt);

    std::ostringstream oss;
    oss << std::put_time(&bt, "%Y-%m-%dT%H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
}


// ======================================================================
// 5. THREAD-SAFE DATABASE LOGGING  (Async Queue + Dedicated Worker)
// ======================================================================

/**
 * Thread-safe, non-blocking telemetry logger.
 *
 * - Callers push log entries onto a thread-safe queue.
 * - A single background worker thread drains the queue and performs
 *   INSERT operations on a dedicated SQLite connection, preventing
 *   both the "database is locked" errors and the bottleneck of
 *   synchronous writes inside hot-path lock operations.
 */
class TelemetryLogger {
public:
    explicit TelemetryLogger(const char* dbPath = DB_FILE)
        : dbPath_(dbPath), shutdown_(false)
    {
        worker_ = std::thread(&TelemetryLogger::drainLoop, this);
    }

    ~TelemetryLogger() { shutdown(); }

    // -- Public API ----------------------------------------------------

    /** Enqueue a telemetry event (non-blocking). */
    void logAction(const std::string& txnId,
                   const std::string& action,
                   const std::string& resourceId)
    {
        std::string ts = nowISO();
        {
            std::lock_guard<std::mutex> lk(mu_);
            queue_.push({"LOG", ts, txnId, action, resourceId});
        }
        cv_.notify_one();
    }

    /** INSERT a transaction row into the Transactions table. */
    void registerTransaction(const Transaction& txn) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            queue_.push({"REGISTER", txn.txn_id,
                         std::to_string(txn.priority),
                         txnStatusToStr(txn.status), ""});
        }
        cv_.notify_one();
    }

    /** UPDATE a transaction's Final_Status. */
    void updateTransactionStatus(const std::string& txnId, TxnStatus status) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            queue_.push({"UPDATE", txnId, txnStatusToStr(status), "", ""});
        }
        cv_.notify_one();
    }

    /** Signal the worker to finish draining and wait for it. */
    void shutdown(double /*timeoutSec*/ = 5.0) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (shutdown_) return;
            shutdown_ = true;
        }
        cv_.notify_one();
        if (worker_.joinable()) worker_.join();
    }

private:
    // Each queue item is a tuple of 5 strings.
    //   [0] = tag:  "LOG" | "REGISTER" | "UPDATE"
    //   For LOG:       [1]=timestamp  [2]=txn_id  [3]=action     [4]=resource_id
    //   For REGISTER:  [1]=txn_id     [2]=priority [3]=status    [4]=""
    //   For UPDATE:    [1]=txn_id     [2]=status   [3]=""        [4]=""
    struct Entry {
        std::string tag, a, b, c, d;
    };

    std::string                dbPath_;
    std::queue<Entry>          queue_;
    std::mutex                 mu_;
    std::condition_variable    cv_;
    bool                       shutdown_;
    std::thread                worker_;

    void drainLoop() {
        sqlite3* db = nullptr;
        if (sqlite3_open(dbPath_.c_str(), &db) != SQLITE_OK) {
            std::cerr << "[TelemetryWorker] Cannot open DB: "
                      << sqlite3_errmsg(db) << "\n";
            sqlite3_close(db);
            return;
        }
        sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);

        while (true) {
            Entry item;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [&]{ return shutdown_ || !queue_.empty(); });
                if (queue_.empty() && shutdown_) break;
                if (queue_.empty()) continue;
                item = queue_.front();
                queue_.pop();
            }

            char* errMsg = nullptr;
            if (item.tag == "REGISTER") {
                std::string sql =
                    "INSERT OR IGNORE INTO Transactions "
                    "(Transaction_ID, Priority, Final_Status) VALUES ('" +
                    item.a + "', " + item.b + ", '" + item.c + "');";
                sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);

            } else if (item.tag == "UPDATE") {
                std::string sql =
                    "UPDATE Transactions SET Final_Status = '" + item.b +
                    "' WHERE Transaction_ID = '" + item.a + "';";
                sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);

            } else {  // "LOG"
                std::string sql =
                    "INSERT INTO Telemetry_Log "
                    "(Timestamp, Transaction_ID, Action, Resource_ID) VALUES ('" +
                    item.a + "', '" + item.b + "', '" + item.c + "', '" + item.d + "');";
                sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
            }

            if (errMsg) {
                std::cerr << "[TelemetryWorker] DB error: " << errMsg << "\n";
                sqlite3_free(errMsg);
            }
        }

        sqlite3_close(db);
    }
};


// ======================================================================
// 2 & 3. LOCK TABLE + ACQUISITION / RELEASE / ROLLBACK  (Thread-Safe)
// ======================================================================

/**
 * Central lock-table manager guarded by a single mutex.
 *
 * Every public method acquires `mutex_` before reading or writing the
 * shared `lock_table` or any Transaction/Resource object, guaranteeing
 * freedom from data-race conditions when multiple transaction threads
 * call these methods concurrently.
 *
 * Integration point for WFG / OS code:
 *   - getWaitingFor()   returns a snapshot of (txn -> resource_it_waits_for)
 *   - getResourceOwner()  returns the owner of a given resource
 *   These two together let you build a Wait-For Graph externally.
 */
class LockManager {
public:
    explicit LockManager(TelemetryLogger& logger) : logger_(logger) {}

    // -- Helpers -------------------------------------------------------

    /** Pre-register a resource so it exists in the lock table. */
    void registerResource(const std::string& resourceId) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (lockTable_.find(resourceId) == lockTable_.end()) {
            lockTable_[resourceId] = Resource{resourceId, ""};
        }
    }

    /** Register a new transaction and persist it in the DB. */
    void registerTransaction(Transaction& txn) {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            transactions_[txn.txn_id] = txn;
        }
        logger_.registerTransaction(txn);
    }

    // -- Req. 2a: Lock Acquisition -------------------------------------

    /**
     * Try to acquire `resourceId` on behalf of `txnId`.
     *
     * Returns:
     *   true  -- lock granted immediately.
     *   false -- resource is owned by another transaction;
     *            the requesting transaction is moved to WAITING.
     */
    bool requestLock(const std::string& txnId, const std::string& resourceId) {
        std::lock_guard<std::mutex> lk(mutex_);
        auto& txn      = transactions_[txnId];
        auto& resource  = lockTable_[resourceId];

        // Guard: don't let an aborted transaction acquire locks.
        if (txn.status == TxnStatus::ABORTED) {
            std::cout << "  [DENIED]   " << txnId
                      << " is ABORTED -- lock request for "
                      << resourceId << " rejected.\n";
            return false;
        }

        if (resource.isFree()) {
            // -- Grant the lock ----------------------------------------
            resource.owner_txn_id = txnId;
            txn.held_resources.push_back(resourceId);
            txn.status      = TxnStatus::RUNNING;
            txn.waiting_for.clear();
            std::cout << "  [ACQUIRED] " << txnId
                      << " locked " << resourceId << "\n";
            logger_.logAction(txnId, "Acquire", resourceId);
            return true;

        } else if (resource.owner_txn_id == txnId) {
            // Re-entrant: transaction already owns the resource.
            std::cout << "  [NO-OP]    " << txnId
                      << " already owns " << resourceId << ".\n";
            return true;

        } else {
            // -- Conflict: resource held by another txn ----------------
            const std::string& ownerId = resource.owner_txn_id;
            txn.status      = TxnStatus::WAITING;
            txn.waiting_for = resourceId;
            std::cout << "  [WAITING]  " << txnId
                      << " blocked on " << resourceId
                      << " (held by " << ownerId << ")\n";
            logger_.logAction(txnId, "Wait", resourceId);
            return false;
        }
    }

    // -- Req. 2b: Lock Release -----------------------------------------

    /** Release a single lock and update the lock table. */
    void releaseLock(const std::string& txnId, const std::string& resourceId) {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = lockTable_.find(resourceId);
        if (it == lockTable_.end() || it->second.owner_txn_id != txnId)
            return;  // nothing to release

        it->second.owner_txn_id.clear();
        auto& txn = transactions_[txnId];
        auto pos  = std::find(txn.held_resources.begin(),
                              txn.held_resources.end(), resourceId);
        if (pos != txn.held_resources.end())
            txn.held_resources.erase(pos);

        std::cout << "  [RELEASED] " << txnId
                  << " released " << resourceId << "\n";
        logger_.logAction(txnId, "Release", resourceId);
    }

    // -- Req. 2c: Rollback / Dynamic Lock Release ----------------------

    /**
     * Forcefully abort a "victim" transaction:
     *   - Release every lock it holds.
     *   - Set its status to ABORTED.
     * This is the Dynamic Lock Release mechanism used by the deadlock
     * detector to break cycles in the Wait-For Graph.
     */
    void rollbackTransaction(const std::string& txnId) {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = transactions_.find(txnId);
        if (it == transactions_.end()) return;

        auto& txn = it->second;
        std::cout << "\n  [ROLLBACK] Rolling back " << txnId
                  << " -- releasing all held locks ...\n";

        for (const auto& rid : txn.held_resources) {
            auto resIt = lockTable_.find(rid);
            if (resIt != lockTable_.end() &&
                resIt->second.owner_txn_id == txnId) {
                resIt->second.owner_txn_id.clear();
                std::cout << "             -> freed " << rid << "\n";
                logger_.logAction(txnId, "Rollback", rid);
            }
        }

        txn.held_resources.clear();
        txn.status = TxnStatus::ABORTED;
        txn.waiting_for.clear();
        logger_.updateTransactionStatus(txnId, TxnStatus::ABORTED);
    }

    // -- Convenience: Commit -------------------------------------------

    /** Mark a transaction as committed and release all its locks. */
    void commitTransaction(const std::string& txnId) {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = transactions_.find(txnId);
        if (it == transactions_.end() || it->second.status == TxnStatus::ABORTED)
            return;

        auto& txn = it->second;
        for (const auto& rid : txn.held_resources) {
            auto resIt = lockTable_.find(rid);
            if (resIt != lockTable_.end() &&
                resIt->second.owner_txn_id == txnId) {
                resIt->second.owner_txn_id.clear();
                logger_.logAction(txnId, "Release", rid);
            }
        }

        txn.held_resources.clear();
        txn.status = TxnStatus::COMMITTED;
        txn.waiting_for.clear();
        std::cout << "  [COMMIT]   " << txnId
                  << " committed -- all locks released.\n";
        logger_.updateTransactionStatus(txnId, TxnStatus::COMMITTED);
    }

    // -- WFG / OS Integration Hooks ------------------------------------

    /**
     * Returns a map  txn_id -> resource_id  for every transaction
     * currently in WAITING state.  Use this to build a Wait-For Graph.
     */
    std::map<std::string, std::string> getWaitingFor() const {
        std::lock_guard<std::mutex> lk(mutex_);
        std::map<std::string, std::string> result;
        for (const auto& [tid, txn] : transactions_) {
            if (txn.status == TxnStatus::WAITING && !txn.waiting_for.empty()) {
                result[tid] = txn.waiting_for;
            }
        }
        return result;
    }

    /**
     * Returns the owner txn_id of a resource, or "" if free.
     * Together with getWaitingFor(), this provides all the edges
     * for a Wait-For Graph:  waiter -> owner.
     */
    std::string getResourceOwner(const std::string& resourceId) const {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = lockTable_.find(resourceId);
        if (it == lockTable_.end()) return "";
        return it->second.owner_txn_id;
    }

    /**
     * Returns a snapshot of all registered transactions (copy).
     * Useful for the WFG module or for external status display.
     */
    std::map<std::string, Transaction> getTransactions() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return transactions_;
    }

    // -- Snapshot (for debugging) --------------------------------------

    /** Return a human-readable snapshot of the current state. */
    std::string snapshot() const {
        std::lock_guard<std::mutex> lk(mutex_);
        std::ostringstream oss;
        oss << "\n+--- Lock Table Snapshot -----------------------------------+\n";
        for (const auto& [rid, res] : lockTable_)
            oss << "|  " << res.repr() << "\n";
        oss << "+--- Transactions -----------------------------------------+\n";
        for (const auto& [tid, txn] : transactions_)
            oss << "|  " << txn.repr() << "\n";
        oss << "+----------------------------------------------------------+\n";
        return oss.str();
    }

private:
    std::map<std::string, Resource>    lockTable_;      // resource_id -> Resource
    std::map<std::string, Transaction> transactions_;   // txn_id -> Transaction
    mutable std::mutex                 mutex_;           // guards ALL shared state
    TelemetryLogger&                   logger_;
};


// ======================================================================
// SIMULATION THREAD FUNCTION
// ======================================================================

/**
 * Simulates a transaction thread that tries to acquire each resource
 * in `resourceSequence` in order, holds it briefly, and then commits.
 *
 * If a lock request fails (resource owned by another thread), the
 * transaction enters WAITING state -- exactly the condition needed for
 * the upstream deadlock detector to find cycles.
 */
static void simulateTransaction(
    LockManager& lm,
    const std::string& txnId,
    const std::vector<std::string>& resourceSequence,
    int holdTimeMs = 300)
{
    for (const auto& rid : resourceSequence) {
        bool granted = lm.requestLock(txnId, rid);
        if (!granted) {
            // In a full system the deadlock detector would resolve this.
            // Here we just pause briefly to simulate the blocking wait.
            std::this_thread::sleep_for(std::chrono::milliseconds(holdTimeMs));

            // Retry once after the wait (the resource may have been freed
            // by a rollback or by the owner committing).
            granted = lm.requestLock(txnId, rid);
            if (!granted) {
                std::cout << "  [BLOCKED]  " << txnId
                          << " still blocked on " << rid
                          << " -- skipping resource.\n";
                continue;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(holdTimeMs));
    }

    // Commit (releases all remaining locks).
    lm.commitTransaction(txnId);
}


// ======================================================================
// DATABASE DUMP UTILITY
// ======================================================================

/** Pretty-print the two DB tables to prove persistence. */
static void printDbContents(const char* dbPath = DB_FILE) {
    sqlite3* db = nullptr;
    if (sqlite3_open(dbPath, &db) != SQLITE_OK) {
        std::cerr << "[DB] Cannot open for dump: " << sqlite3_errmsg(db) << "\n";
        sqlite3_close(db);
        return;
    }

    std::cout << "\n+==============================================================+\n"
              <<   "|                  FINAL DATABASE CONTENTS                      |\n"
              <<   "+==============================================================+\n"
              <<   "|  TABLE: Transactions                                         |\n"
              <<   "+--------------------------------------------------------------+\n";

    // Column header
    std::printf("|  %-18s %-10s %-14s  |\n",
                "Transaction_ID", "Priority", "Final_Status");
    std::cout << "|  " << std::string(42, '-') << "              |\n";

    sqlite3_stmt* stmt = nullptr;
    sqlite3_prepare_v2(db,
        "SELECT Transaction_ID, Priority, Final_Status "
        "FROM Transactions ORDER BY Transaction_ID;",
        -1, &stmt, nullptr);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* tid    = (const char*)sqlite3_column_text(stmt, 0);
        int         pri    = sqlite3_column_int(stmt, 1);
        const char* status = (const char*)sqlite3_column_text(stmt, 2);
        std::printf("|  %-18s %-10d %-14s  |\n", tid, pri, status);
    }
    sqlite3_finalize(stmt);

    std::cout << "+--------------------------------------------------------------+\n"
              << "|  TABLE: Telemetry_Log                                        |\n"
              << "+--------------------------------------------------------------+\n";
    std::printf("|  %-26s %-10s %-10s %-8s  |\n",
                "Timestamp", "TxnID", "Action", "Resource");
    std::cout << "|  " << std::string(54, '-') << "        |\n";

    sqlite3_prepare_v2(db,
        "SELECT Timestamp, Transaction_ID, Action, Resource_ID "
        "FROM Telemetry_Log ORDER BY id;",
        -1, &stmt, nullptr);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char* ts   = (const char*)sqlite3_column_text(stmt, 0);
        const char* tid  = (const char*)sqlite3_column_text(stmt, 1);
        const char* act  = (const char*)sqlite3_column_text(stmt, 2);
        const char* res  = (const char*)sqlite3_column_text(stmt, 3);
        std::printf("|  %-26s %-10s %-10s %-8s  |\n", ts, tid, act, res);
    }
    sqlite3_finalize(stmt);

    std::cout << "+==============================================================+\n";
    sqlite3_close(db);
}


// ======================================================================
// INTERACTIVE USER INPUT HELPERS
// ======================================================================

/** Prompt the user until a valid positive integer is entered. */
static int getPositiveInt(const std::string& prompt) {
    while (true) {
        std::cout << prompt;
        std::string raw;
        std::getline(std::cin, raw);
        // Trim whitespace
        raw.erase(0, raw.find_first_not_of(" \t\r\n"));
        raw.erase(raw.find_last_not_of(" \t\r\n") + 1);
        try {
            int val = std::stoi(raw);
            if (val > 0) return val;
        } catch (...) {}
        std::cout << "  [ERROR] Please enter a positive integer.\n";
    }
}

/** Convert a string to uppercase in-place and return it. */
static std::string toUpper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    return s;
}

/**
 * Prompt the user for a comma-separated list of resource IDs.
 * Validates each entry against the registered resource pool.
 */
static std::vector<std::string> getResourceList(
    const std::string& prompt,
    const std::vector<std::string>& validResources)
{
    while (true) {
        std::cout << prompt;
        std::string raw;
        std::getline(std::cin, raw);
        raw.erase(0, raw.find_first_not_of(" \t\r\n"));
        raw.erase(raw.find_last_not_of(" \t\r\n") + 1);
        if (raw.empty()) {
            std::cout << "  [ERROR] You must request at least one resource.\n";
            continue;
        }

        std::vector<std::string> items;
        std::istringstream ss(raw);
        std::string token;
        while (std::getline(ss, token, ',')) {
            token.erase(0, token.find_first_not_of(" \t"));
            token.erase(token.find_last_not_of(" \t") + 1);
            items.push_back(toUpper(token));
        }

        // Validate
        std::vector<std::string> invalid;
        for (const auto& r : items) {
            if (std::find(validResources.begin(), validResources.end(), r)
                == validResources.end()) {
                invalid.push_back(r);
            }
        }
        if (!invalid.empty()) {
            std::cout << "  [ERROR] Unknown resource(s): [";
            for (size_t i = 0; i < invalid.size(); ++i) {
                if (i) std::cout << ", ";
                std::cout << invalid[i];
            }
            std::cout << "]. Choose from [";
            for (size_t i = 0; i < validResources.size(); ++i) {
                if (i) std::cout << ", ";
                std::cout << validResources[i];
            }
            std::cout << "].\n";
            continue;
        }
        return items;
    }
}


// ======================================================================
// MAIN -- INTERACTIVE SIMULATION
// ======================================================================

int main() {
    std::cout << std::string(64, '=') << "\n"
              << "  AI-Based Deadlock Detection -- Lock Management Prototype\n"
              << std::string(64, '=') << "\n\n";

    // -----------------------------------------------------------------
    // Step 1: Collect resource definitions from the user
    // -----------------------------------------------------------------
    int numResources = getPositiveInt(
        "  Enter the number of shared resources to create: ");

    std::vector<std::string> resources;
    for (int i = 1; i <= numResources; ++i)
        resources.push_back("R" + std::to_string(i));

    std::cout << "  [INFO] Created resource pool: [";
    for (size_t i = 0; i < resources.size(); ++i) {
        if (i) std::cout << ", ";
        std::cout << resources[i];
    }
    std::cout << "]\n\n";

    // -----------------------------------------------------------------
    // Step 2: Collect transaction definitions from the user
    // -----------------------------------------------------------------
    int numTxns = getPositiveInt(
        "  Enter the number of concurrent transactions to simulate: ");

    // (txn_id, priority, resource_sequence)
    struct TxnConfig {
        std::string              txnId;
        int                      priority;
        std::vector<std::string> resSeq;
    };
    std::vector<TxnConfig> txnConfigs;

    for (int i = 1; i <= numTxns; ++i) {
        std::string txnId = "T" + std::to_string(i);
        std::cout << "\n  --- Configure " << txnId << " ---\n";
        int priority = getPositiveInt(
            "    Priority for " + txnId + " (1 = highest): ");
        auto resSeq  = getResourceList(
            "    Resources " + txnId +
            " should request (comma-separated, e.g. R1,R3,R2): ",
            resources);
        txnConfigs.push_back({txnId, priority, resSeq});
    }

    // -----------------------------------------------------------------
    // Step 3: Ask the user which transaction to rollback as victim
    // -----------------------------------------------------------------
    std::vector<std::string> validTxnIds;
    for (const auto& cfg : txnConfigs) validTxnIds.push_back(cfg.txnId);

    std::cout << "\n  Active transactions: [";
    for (size_t i = 0; i < validTxnIds.size(); ++i) {
        if (i) std::cout << ", ";
        std::cout << validTxnIds[i];
    }
    std::cout << "]\n";

    std::string victimId;
    while (true) {
        std::cout << "  Enter the Transaction ID to rollback as the deadlock victim "
                  << "(e.g. " << validTxnIds.back() << "): ";
        std::getline(std::cin, victimId);
        victimId.erase(0, victimId.find_first_not_of(" \t\r\n"));
        victimId.erase(victimId.find_last_not_of(" \t\r\n") + 1);
        victimId = toUpper(victimId);
        if (std::find(validTxnIds.begin(), validTxnIds.end(), victimId)
            != validTxnIds.end())
            break;
        std::cout << "  [ERROR] Invalid ID. Choose from [";
        for (size_t i = 0; i < validTxnIds.size(); ++i) {
            if (i) std::cout << ", ";
            std::cout << validTxnIds[i];
        }
        std::cout << "].\n";
    }
    std::cout << "\n";

    // -----------------------------------------------------------------
    // Step 4: Initialise DB + Logger + Lock Manager
    // -----------------------------------------------------------------
    initDatabase();
    TelemetryLogger logger;
    LockManager lm(logger);

    for (const auto& r : resources) lm.registerResource(r);
    std::cout << "  [INIT] Registered resources: [";
    for (size_t i = 0; i < resources.size(); ++i) {
        if (i) std::cout << ", ";
        std::cout << resources[i];
    }
    std::cout << "]\n";

    std::vector<Transaction> txns;
    for (const auto& cfg : txnConfigs) {
        Transaction txn;
        txn.txn_id   = cfg.txnId;
        txn.priority = cfg.priority;
        txns.push_back(txn);
        lm.registerTransaction(txns.back());
    }
    std::cout << "  [INIT] Registered transactions: [";
    for (size_t i = 0; i < txns.size(); ++i) {
        if (i) std::cout << ", ";
        std::cout << txns[i].txn_id;
    }
    std::cout << "]\n\n";

    // -----------------------------------------------------------------
    // Step 5: Display the execution plan
    // -----------------------------------------------------------------
    std::cout << std::string(64, '-') << "\n"
              << "  Execution Plan\n"
              << std::string(64, '-') << "\n";

    for (const auto& cfg : txnConfigs) {
        std::cout << "    " << cfg.txnId
                  << " (priority " << cfg.priority << "):  ";
        for (size_t i = 0; i < cfg.resSeq.size(); ++i) {
            if (i) std::cout << " -> ";
            std::cout << cfg.resSeq[i];
        }
        std::cout << "\n";
    }
    std::cout << "    Victim for rollback:  " << victimId << "\n"
              << std::string(64, '-') << "\n\n";

    // -----------------------------------------------------------------
    // Step 6: Spawn transaction threads
    // -----------------------------------------------------------------
    std::cout << std::string(64, '-') << "\n"
              << "  Spawning transaction threads ...\n"
              << std::string(64, '-') << "\n";

    std::vector<std::thread> threads;
    for (const auto& cfg : txnConfigs) {
        threads.emplace_back(simulateTransaction,
                             std::ref(lm),
                             cfg.txnId,
                             cfg.resSeq,
                             250);
    }

    // Give threads a moment to reach their first lock requests.
    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    // -----------------------------------------------------------------
    // Step 7: Perform the user-selected rollback
    // -----------------------------------------------------------------
    std::cout << "\n" << std::string(64, '-') << "\n"
              << "  [Deadlock Detector] Rolling back victim " << victimId << " ...\n"
              << std::string(64, '-') << "\n";
    lm.rollbackTransaction(victimId);

    // Print a mid-run snapshot.
    std::cout << lm.snapshot();

    // -----------------------------------------------------------------
    // Step 8: Wait for all threads to finish
    // -----------------------------------------------------------------
    for (auto& t : threads)
        t.join();

    // -----------------------------------------------------------------
    // Step 9: Final snapshot
    // -----------------------------------------------------------------
    std::cout << "\n" << std::string(64, '-') << "\n"
              << "  All threads finished.  Final state:\n"
              << std::string(64, '-') << "\n"
              << lm.snapshot();

    // -----------------------------------------------------------------
    // Step 10: Flush the telemetry logger and dump DB
    // -----------------------------------------------------------------
    logger.shutdown();
    printDbContents();

    std::cout << "\n  [DONE] Simulation complete.\n\n";
    return 0;
}
