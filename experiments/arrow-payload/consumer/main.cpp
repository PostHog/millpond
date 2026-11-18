// arrow-consumer: Kafka (Arrow IPC) -> DuckDB (Arrow C Data Interface) -> DuckLake
//
// PoC for experiments/arrow-payload. See ../README.md for the shared contract.
//
// Buffer lifetime strategy: Option A (simplest).
//   librdkafka owns msg->payload until rd_kafka_message_destroy(). We wrap
//   the buffer in a non-owning arrow::Buffer, feed it through
//   BufferReader -> RecordBatchStreamReader -> ExportRecordBatchReader
//   -> duckdb_arrow_scan -> INSERT SELECT, all synchronously in one loop
//   iteration. rd_kafka_message_destroy() is the LAST call in the iteration,
//   after DuckDB has fully materialized the batch into its own column store
//   during INSERT. Defensible zero-copy without Option B's custom subclass.

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>

#include <duckdb.h>
#include <librdkafka/rdkafka.h>

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

std::atomic<int> g_running{1};
void handle_signal(int) { g_running.store(0); }

const char *env_or(const char *k, const char *fb) {
    const char *v = std::getenv(k);
    return (v && *v) ? v : fb;
}
std::string env_required(const char *k) {
    const char *v = std::getenv(k);
    if (!v || !*v) { std::cerr << "[arrow-consumer] ERROR missing env " << k << "\n"; std::exit(2); }
    return v;
}
void log_err(const std::string &m) { std::cerr << "[arrow-consumer] ERROR " << m << "\n"; }

void duckdb_exec(duckdb_connection con, const std::string &sql) {
    duckdb_result res;
    if (duckdb_query(con, sql.c_str(), &res) != DuckDBSuccess) {
        std::string err = duckdb_result_error(&res) ? duckdb_result_error(&res) : "(null)";
        duckdb_destroy_result(&res);
        throw std::runtime_error("duckdb_query failed: " + sql + " : " + err);
    }
    duckdb_destroy_result(&res);
}

void init_ducklake(duckdb_connection con) {
    // Extensions: httpfs MUST load before ducklake (see AGENT.md).
    duckdb_exec(con, "LOAD httpfs");
    duckdb_exec(con, "LOAD ducklake");
    duckdb_exec(con, "LOAD postgres");

    auto set_opt = [&](const char *key, const char *val, bool quoted) {
        if (!val || !*val) return;
        std::string sql = std::string("SET ") + key + "=";
        sql += quoted ? (std::string("'") + val + "'") : val;
        duckdb_exec(con, sql);
    };
    set_opt("s3_endpoint",          std::getenv("DUCKDB_S3_ENDPOINT"),          true);
    set_opt("s3_access_key_id",     std::getenv("DUCKDB_S3_ACCESS_KEY_ID"),     true);
    set_opt("s3_secret_access_key", std::getenv("DUCKDB_S3_SECRET_ACCESS_KEY"), true);
    set_opt("s3_use_ssl",           env_or("DUCKDB_S3_USE_SSL", "false"),       false);
    set_opt("s3_url_style",         env_or("DUCKDB_S3_URL_STYLE", "path"),      true);

    // Postgres connstring values are unquoted: any single quote inside would
    // collide with the SQL string literal that wraps the ATTACH target. The
    // values in this experiment are alphanumeric (no spaces, no quotes), so
    // unquoted is safe and avoids the escape rabbit hole entirely.
    std::string pg = "host=" + env_required("DUCKLAKE_RDS_HOST") +
                     " port=" + env_or("DUCKLAKE_RDS_PORT", "5432") +
                     " dbname=" + env_required("DUCKLAKE_RDS_DATABASE") +
                     " user=" + env_required("DUCKLAKE_RDS_USERNAME") +
                     " password=" + env_required("DUCKLAKE_RDS_PASSWORD");
    duckdb_exec(con, "ATTACH 'ducklake:postgres:" + pg + "' AS lake (DATA_PATH '" +
                         env_required("DUCKLAKE_DATA_PATH") + "')");
    duckdb_exec(con,
        "CREATE TABLE IF NOT EXISTS lake.main.events_arrow ("
        " uuid VARCHAR, event VARCHAR, distinct_id VARCHAR, timestamp VARCHAR,"
        " team_id BIGINT, project_id BIGINT, properties VARCHAR, elements_chain VARCHAR,"
        " _inserted_at TIMESTAMP)");
}

// Open an Arrow IPC stream reader over msg->payload (non-owning, zero-copy).
// Caller must keep `msg` alive until the returned reader is dropped.
arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>
open_ipc_reader(const rd_kafka_message_t *msg) {
    auto buf = std::make_shared<arrow::Buffer>(
        static_cast<const uint8_t *>(msg->payload), static_cast<int64_t>(msg->len));
    auto input = std::make_shared<arrow::io::BufferReader>(buf);
    ARROW_ASSIGN_OR_RAISE(auto r, arrow::ipc::RecordBatchStreamReader::Open(input));
    return std::static_pointer_cast<arrow::RecordBatchReader>(r);
}

void ingest(duckdb_connection con,
            std::shared_ptr<arrow::RecordBatchReader> reader,
            const std::string &view) {
    // Lifetime invariant: c_stream is stack-allocated. It MUST remain live
    // through both arrow_scan (which only registers the view) AND the INSERT
    // that drives the lazy arrow_scan pull. Do not split these two calls.
    ArrowArrayStream c_stream{};
    auto st = arrow::ExportRecordBatchReader(reader, &c_stream);
    if (!st.ok()) throw std::runtime_error("ExportRecordBatchReader: " + st.ToString());

    // duckdb_arrow_stream is a typedef'd opaque handle, but at the C ABI
    // boundary DuckDB does `reinterpret_cast<ArrowArrayStream *>(arrow)` on
    // its third argument (see duckdb/src/main/capi/arrow-c.cpp). So we hand
    // it our ArrowArrayStream pointer cast to the typedef. NOT a wrapper
    // struct — that was a misreading of the typedef and would segfault when
    // DuckDB called the wrapper's first word as get_schema().
    if (duckdb_arrow_scan(con, view.c_str(),
                          reinterpret_cast<duckdb_arrow_stream>(&c_stream)) != DuckDBSuccess) {
        if (c_stream.release) c_stream.release(&c_stream);
        throw std::runtime_error("duckdb_arrow_scan failed");
    }
    try {
        duckdb_exec(con, "INSERT INTO lake.main.events_arrow "
                         "SELECT *, NOW() AS _inserted_at FROM \"" + view + "\"");
    } catch (...) {
        duckdb_exec(con, "DROP VIEW IF EXISTS \"" + view + "\"");
        if (c_stream.release) c_stream.release(&c_stream);
        throw;
    }
    duckdb_exec(con, "DROP VIEW IF EXISTS \"" + view + "\"");
    if (c_stream.release) c_stream.release(&c_stream);
}

void commit_offset(rd_kafka_t *rk, const std::string &topic, int32_t part, int64_t off) {
    rd_kafka_topic_partition_list_t *cpl = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(cpl, topic.c_str(), part)->offset = off + 1;
    rd_kafka_resp_err_t err = rd_kafka_commit(rk, cpl, 0 /* sync */);
    rd_kafka_topic_partition_list_destroy(cpl);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) log_err(std::string("commit: ") + rd_kafka_err2str(err));
}

}  // namespace

int main() {
    std::signal(SIGTERM, handle_signal);
    std::signal(SIGINT, handle_signal);

    std::string brokers = env_required("KAFKA_BOOTSTRAP_SERVERS");
    std::string topic   = env_or("KAFKA_TOPIC", "test-events-arrow");
    int partition_count = std::atoi(env_or("KAFKA_PARTITION_COUNT", "8"));
    if (partition_count <= 0) partition_count = 8;

    duckdb_database db; duckdb_connection con;
    if (duckdb_open(nullptr, &db) != DuckDBSuccess) { log_err("duckdb_open"); return 1; }
    if (duckdb_connect(db, &con) != DuckDBSuccess) { log_err("duckdb_connect"); duckdb_close(&db); return 1; }
    try { init_ducklake(con); }
    catch (const std::exception &e) {
        log_err(std::string("ducklake init: ") + e.what());
        duckdb_disconnect(&con); duckdb_close(&db); return 1;
    }

    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    auto conf_set = [&](const char *k, const char *v) {
        if (rd_kafka_conf_set(conf, k, v, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            log_err(std::string("conf ") + k + ": " + errstr); std::exit(1);
        }
    };
    conf_set("bootstrap.servers", brokers.c_str());
    conf_set("group.id", env_or("KAFKA_GROUP_ID", "arrow-consumer"));
    conf_set("enable.auto.commit", "false");
    conf_set("enable.auto.offset.store", "false");
    conf_set("auto.offset.reset", "earliest");
    conf_set("queued.max.messages.kbytes", "16384");

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) { log_err(std::string("rd_kafka_new: ") + errstr); return 1; }
    rd_kafka_poll_set_consumer(rk);

    rd_kafka_topic_partition_list_t *parts = rd_kafka_topic_partition_list_new(partition_count);
    for (int p = 0; p < partition_count; ++p)
        rd_kafka_topic_partition_list_add(parts, topic.c_str(), p)->offset = RD_KAFKA_OFFSET_STORED;
    if (rd_kafka_assign(rk, parts) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        log_err("rd_kafka_assign"); rd_kafka_topic_partition_list_destroy(parts);
        rd_kafka_destroy(rk); return 1;
    }
    rd_kafka_topic_partition_list_destroy(parts);

    std::cerr << "[arrow-consumer] started brokers=" << brokers
              << " topic=" << topic << " partitions=" << partition_count << "\n";

    // --- Main loop ---
    // Lifetime invariant: `msg` MUST outlive all Arrow/DuckDB use of the
    // payload buffer within each iteration. rd_kafka_message_destroy() is the
    // last call before looping.
    uint64_t batch_counter = 0;
    while (g_running.load()) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(rk, 100);
        if (!msg) continue;

        if (msg->err) {
            if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                log_err(std::string("poll: ") + rd_kafka_message_errstr(msg));
            rd_kafka_message_destroy(msg);
            continue;
        }

        try {
            auto r = open_ipc_reader(msg);
            if (!r.ok()) throw std::runtime_error("open ipc: " + r.status().ToString());

            // Drain stream into a batch vector so we can report row counts.
            // Batches still reference msg->payload (zero-copy borrowed ptrs).
            // Distinguish "end of stream" (b == nullptr, status ok) from
            // "decode error" — collapsing them would silently swallow
            // truncated/corrupt IPC streams and commit past them.
            std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
            int64_t rows = 0;
            while (true) {
                std::shared_ptr<arrow::RecordBatch> b;
                auto rs = (*r)->ReadNext(&b);
                if (!rs.ok()) throw std::runtime_error("ReadNext: " + rs.ToString());
                if (!b) break;  // end of stream
                rows += b->num_rows();
                batches.push_back(std::move(b));
            }
            if (batches.empty()) { rd_kafka_message_destroy(msg); continue; }

            auto made = arrow::RecordBatchReader::Make(batches, batches.front()->schema());
            if (!made.ok()) throw std::runtime_error("make reader: " + made.status().ToString());

            std::string view = "arrow_batch_" + std::to_string(batch_counter);
            ingest(con, *made, view);
            commit_offset(rk, topic, msg->partition, msg->offset);

            std::cout << "[arrow-consumer] flushed batch " << batch_counter
                      << " records=" << rows
                      << " offset=" << msg->offset
                      << " partition=" << msg->partition << "\n";
            std::cout.flush();
            ++batch_counter;
        } catch (const std::exception &e) {
            log_err("message p=" + std::to_string(msg->partition) +
                    " off=" + std::to_string(msg->offset) + ": " + e.what());
            commit_offset(rk, topic, msg->partition, msg->offset);
        }

        rd_kafka_message_destroy(msg);  // lifetime invariant: last call
    }

    std::cerr << "[arrow-consumer] shutting down\n";
    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return 0;
}
