#pragma once

#include "tinyxml2.h"
#include "metadata.h"
#include "datanode.h"
#include <mutex>
#include <condition_variable>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace ECProject
{
  class Proxy
  {
  public:
    Proxy(std::string ip, int port, std::string networkcore, std::string config_path);
    ~Proxy();
    void run();

    // rpc调用
    std::string checkalive(std::string msg);
    // encode and set
    void encode_and_store_object(PlacementInfo placement);
    // decode and get
    void decode_and_get_object(PlacementInfo placement);
    // delete
    void delete_blocks(DeletePlan);
    // repair
    MainRepairResp main_repair(MainRepairPlan repair_plan);
    void help_repair(HelpRepairPlan repair_plan);
    // merge
    MainRecalResp main_recal(MainRecalPlan recal_plan);
    void help_recal(HelpRecalPlan recal_plan);
    // block relocation
    RelocateResp block_relocation(RelocatePlan reloc_plan);

  private:
    void init_datanodes();
    void write_to_datanode(const char *key, size_t key_len, const char *value, size_t value_len, const char *ip, int port);
    bool read_from_datanode(const char *key, size_t key_len, char *value, size_t value_len, const char *ip, int port);
    void delete_in_datanode(std::string block_id, const char *ip, int port);
    void block_migration(const char *key, size_t key_len, size_t value_len, const char *src_ip, int src_port, const char *dsn_ip, int dsn_port);
    void transfer_to_networkcore(const char *value, size_t value_len);

    std::unordered_map<std::string, std::unique_ptr<coro_rpc::coro_rpc_client>> datanodes_;
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
    int self_cluster_id_;
    int port_;
    int port_for_transfer_data_;
    std::string ip_;
    std::string networkcore_;
    std::string config_path_;
    asio::io_context io_context_{};
    asio::ip::tcp::acceptor acceptor_;
    std::mutex mutex_;
    std::condition_variable cv_;
  };  
}
