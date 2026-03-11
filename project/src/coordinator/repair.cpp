#include "coordinator.h"

namespace ECProject
{
  void Coordinator::do_repair(
          std::vector<unsigned int> failed_ids, int stripe_id,
          RepairResp& response)
  {
    struct timeval start_time, end_time;
    struct timeval m_start_time, m_end_time;
    double repair_time = 0;
    double decoding_time = 0;
    double cross_cluster_time = 0;
    double meta_time = 0;
    int cross_cluster_transfers = 0;
    int io_cnt = 0;
    std::unordered_map<unsigned int, std::vector<int>> failures_map;
    // 检测故障范围
    check_out_failures(stripe_id, failed_ids, failures_map);
    
    for (auto& pair : failures_map) {
      gettimeofday(&start_time, NULL);
      gettimeofday(&m_start_time, NULL);
      Stripe& stripe = stripe_table_[pair.first];
      stripe.ec->placement_rule = ec_schema_.placement_rule;
      stripe.ec->generate_partition();
      find_out_stripe_partitions(pair.first);
      if (IF_DEBUG) {
        std::cout << "Stripe " << stripe.stripe_id << " block placement:\n";
        for (auto& vec : stripe.ec->partition_plan) {
          unsigned int node_id = stripe.blocks2nodes[vec[0]];
          unsigned int cluster_id = node_table_[node_id].map2cluster;
          std::cout << cluster_id << ": ";
          for (int ele : vec) {
            std::cout << "B" << ele << "N" << stripe.blocks2nodes[ele] << " ";
          }
          std::cout << "\n";
        }
        std::cout << "Generating repair plan for failures:" << std::endl;
        for (auto& failure : pair.second) {
          std::cout << failure << " ";
        }
        std::cout << std::endl;
      }
      std::vector<RepairPlan> repair_plans;
      // 生成修复方案
      bool flag = stripe.ec->generate_repair_plan(pair.second, repair_plans,
                                                  ec_schema_.partial_scheme,
                                                  ec_schema_.repair_priority,
                                                  ec_schema_.repair_method);
      if (!flag) {
        response.success = false;
        return;
      }
      if (IF_DEBUG) {
        std::cout << "Repair Plan: " << std::endl;
        for (int i = 0; i < int(repair_plans.size()); i++) {
          RepairPlan& tmp = repair_plans[i];
          std::cout << "> Failed Blocks: ";
          for (int j = 0; 
               j < int(tmp.failure_idxs.size()); j++) {
            std::cout << tmp.failure_idxs[j] << " ";
          }
          std::cout << std::endl;
          std::cout << "> Repair by Blocks: ";
          for (auto& help_blocks : tmp.help_blocks) {
            for(auto& block : help_blocks) {
              std::cout << block << " ";
            }
          }
          std::cout << std::endl;
          std::cout << "> local_or_column: " << tmp.local_or_column << std::endl;
          std::cout << "> Parity idx: ";
          for (auto& idx : tmp.parity_idxs) {
            std::cout << idx << " ";
          }
          std::cout << std::endl;
        }
      }
      std::vector<MainRepairPlan> main_repairs;
      std::vector<std::vector<HelpRepairPlan>> help_repairs;
      // 生成具体的修复方案
      if (check_ec_family(ec_schema_.ec_type) == PCs) { // 乘积码
        concrete_repair_plans_pc(pair.first, repair_plans, main_repairs, help_repairs);
      } else { // 通用处理
        concrete_repair_plans(pair.first, repair_plans, main_repairs, help_repairs);
      }
      
      if (IF_DEBUG) {
        std::cout << "Finish generate repair plan." << std::endl;
      }

      auto lock_ptr = std::make_shared<std::mutex>();

      auto send_main_repair_plan = 
          [this, main_repairs, lock_ptr, &decoding_time, &cross_cluster_time](
              int i, int main_cluster_id) mutable
      {
        std::string chosen_proxy = cluster_table_[main_cluster_id].proxy_ip +
            std::to_string(cluster_table_[main_cluster_id].proxy_port);
        // 向主修复proxy发送修复任务，主修复proxy指故障块最多的集群，负责解码计算
        auto resp = 
            async_simple::coro::syncAwait(proxies_[chosen_proxy]
                ->call_for<&Proxy::main_repair>(
                    std::chrono::seconds{500}, main_repairs[i])).value();
        lock_ptr->lock();
        decoding_time += resp.decoding_time;
        cross_cluster_time += resp.cross_cluster_time;
        lock_ptr->unlock();
        if (IF_DEBUG) {
          std::cout << "Selected main proxy " << chosen_proxy << " of cluster"
                    << main_cluster_id << ". Decoding time : " 
                    << decoding_time << std::endl;
        }
      };

      auto send_help_repair_plan = 
          [this, help_repairs](
              int i, int j, std::string proxy_ip, int proxy_port) mutable
      {
        std::string chosen_proxy = proxy_ip + std::to_string(proxy_port);
        async_simple::coro::syncAwait(proxies_[chosen_proxy]
            ->call_for<&Proxy::help_repair>(std::chrono::seconds{500}, help_repairs[i][j]));
        if (IF_DEBUG) {
          std::cout << "Selected help proxy " << chosen_proxy << std::endl;
        }
      };

      // simulation-统计理论上的跨集群传输量、IO次数
      simulation_repair(main_repairs, cross_cluster_transfers, io_cnt);
      if (IF_DEBUG) {
        std::cout << "Finish simulation! " << cross_cluster_transfers << std::endl;
      }
      gettimeofday(&m_end_time, NULL);
      meta_time += m_end_time.tv_sec - m_start_time.tv_sec +
          (m_end_time.tv_usec - m_start_time.tv_usec) * 1.0 / 1000000;

      if (!IF_SIMULATION) {
        for (int i = 0; i < int(main_repairs.size()); i++) {
          try
          {
            MainRepairPlan& tmp = main_repairs[i];
            int failed_num = int(tmp.failed_blocks_index.size());
            unsigned int main_cluster_id = tmp.cluster_id;
            // 多线程发送修复任务
            std::thread my_main_thread(send_main_repair_plan, i, main_cluster_id);
            std::vector<std::thread> senders;
            int index = 0;
            for (int j = 0; j < int(tmp.help_clusters_blocks_info.size()); j++) {
              int num_of_blocks_in_help_cluster = 
                  (int)tmp.help_clusters_blocks_info[j].size();
              my_assert(num_of_blocks_in_help_cluster == 
                  int(help_repairs[i][j].inner_cluster_help_blocks_info.size()));
              bool t_flag = tmp.help_clusters_partial_less[j];
              if ((IF_DIRECT_FROM_NODE && ec_schema_.partial_decoding
                   && t_flag) ||
                  !IF_DIRECT_FROM_NODE)
              {
                Cluster &cluster = cluster_table_[help_repairs[i][j].cluster_id];
                senders.push_back(std::thread(send_help_repair_plan, i, j,
                                  cluster.proxy_ip, cluster.proxy_port));
              }
            }
            for (int j = 0; j < int(senders.size()); j++) {
              senders[j].join();
            }
            my_main_thread.join();
          }
          catch(const std::exception& e)
          {
            std::cerr << e.what() << '\n';
          }
        }
      }
      // 更新元数据
      for (int i = 0; i < int(main_repairs.size()); i++) {
        MainRepairPlan& tmp = main_repairs[i];
        int j = 0;
        for (auto& idx : tmp.failed_blocks_index) {
          stripe.blocks2nodes[idx] = tmp.new_locations[j++].first;
        }
      }
      gettimeofday(&end_time, NULL);
      double temp_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      repair_time += temp_time;

      if (IF_DEBUG) {
        std::cout << "New blocks placement for stripe " << stripe.stripe_id << ":" << std::endl;
        for (int i = 0; i < int(main_repairs.size()); i++) {
          MainRepairPlan& tmp = main_repairs[i];
          int j = 0;
          for (auto& idx : tmp.failed_blocks_index) {
              unsigned int old_node_id = stripe.blocks2nodes[idx]; // 更新前的节点
              unsigned int old_cluster_id = node_table_[old_node_id].map2cluster;
              unsigned int new_node_id = tmp.new_locations[j].first;
              unsigned int new_cluster_id = node_table_[new_node_id].map2cluster;
              std::cout << "  Block " << idx << ": from node " << old_node_id
                        << " (cluster " << old_cluster_id << ") to node " << new_node_id
                        << " (cluster " << new_cluster_id << ")" << std::endl;
              j++;
          }
        }
      }

      std::cout << "Repair[ ";
      for (auto& failure : pair.second) {
        std::cout << failure << " ";
      }
      std::cout << "]: total = " << repair_time << "s, latest = "
                << temp_time << "s. Decode: total = "
                << decoding_time << std::endl;
    }
    response.decoding_time = decoding_time;
    response.cross_cluster_time = cross_cluster_time;
    response.repair_time = repair_time;
    response.meta_time = meta_time;
    response.cross_cluster_transfers = cross_cluster_transfers;
    response.io_cnt = io_cnt;
    response.success = true;
  }

  // check_out_failures：检测故障影响范围
  // stripe_id ≥ 0：修复指定条带的故障块（块故障）
  // stripe_id = -1：修复指定节点上的所有故障块（节点故障）
  void Coordinator::check_out_failures(
          int stripe_id, std::vector<unsigned int> failed_ids,
          std::unordered_map<unsigned int, std::vector<int>> &failures_map)
  {
    if (stripe_id >= 0) {   // block failures
      for (auto id : failed_ids) {
        failures_map[stripe_id].push_back((int)id);
      }
    } else {   // node failures，输出：{条带ID: [该节点在该条带中的块索引列表]}
      int num_of_failed_nodes = int(failed_ids.size());
      int repair_stripe_num = ec_schema_.repair_stripe_num;

      for (int i = 0; i < num_of_failed_nodes; i++) {
        unsigned int node_id = failed_ids[i];
        // 遍历所有条带，找到故障节点上的所有块
        for (auto it = stripe_table_.begin(); it != stripe_table_.end(); it++) {
          int t_stripe_id = it->first;
          auto& stripe = it->second;
          // [修改1] 使用 blocks2nodes 的实际大小作为边界，避免越界访问产生未定义行为
          int n = stripe.blocks2nodes.size();
          // int n = stripe.ec->k + stripe.ec->m;
          // [修改2] 使用 vector 收集所有匹配的块，替代原先只找第一个就 break 的逻辑
          std::vector<int> current_failed_blocks;
          for (int j = 0; j < n; j++) {
            if (stripe.blocks2nodes[j] == node_id) {
              current_failed_blocks.push_back(j);
            }
          }
          if (!current_failed_blocks.empty()) {
            auto map_it = failures_map.find(t_stripe_id);
            if (map_it != failures_map.end()) {
              // 如果已存在该条带，直接追加新的故障块索引
              for (int idx : current_failed_blocks) {
                map_it->second.push_back(idx);
              }
            } else {
              if (failures_map.size() < (int)repair_stripe_num) {
                failures_map[t_stripe_id] = current_failed_blocks;
              } else {
                continue;
              }
            }
          }

          // int failed_block_idx = -1;
          // // 查找该条带中的块是否有在故障节点node_id中的
          // for (int j = 0; j < n; j++) {
          //   if (stripe.blocks2nodes[j] == node_id) {
          //     failed_block_idx = j;
          //     break;
          //   }
          // }
          // if (failed_block_idx != -1) {
          //   auto map_it = failures_map.find(t_stripe_id);
          //   if (map_it != failures_map.end()) {
          //     map_it->second.push_back(failed_block_idx); // 记录故障块索引
          //   } else {
          //     if (failures_map.size() < (int)repair_stripe_num) {
          //       std::vector<int> failed_blocks;
          //       failed_blocks.push_back(failed_block_idx);
          //       failures_map[t_stripe_id] = failed_blocks;
          //     } else {
          //       continue;
          //     }
          //   }
          // }
          // BUG: 少了判断
          // if (failures_map.find(t_stripe_id) != failures_map.end()) {
          //   failures_map[t_stripe_id].push_back(failed_block_idx); // 记录故障块索引
          // } else {
          //   std::vector<int> failed_blocks;
          //   failed_blocks.push_back(failed_block_idx);
          //   failures_map[t_stripe_id] = failed_blocks;
          // }
        }
      }
    }
    // --- Diagnostic Output ---
    std::cout << "[REPAIR] Total failed stripes to repair: " << failures_map.size() << std::endl;
    if (!failures_map.empty()) {
        std::cout << "[REPAIR] Failed Stripe IDs and their failed block indices: " << std::endl;
        for (auto const& [sid, f_indices] : failures_map) {
            std::cout << "  - Stripe ID " << sid << ": [ ";
            for (int idx : f_indices) {
                std::cout << idx << " ";
            }
            std::cout << "]" << std::endl;
        }
    }
    // -------------------------
  }

  // 为RS码、LRC码生成具体的修复执行计划
  bool Coordinator::concrete_repair_plans(
          int stripe_id,
          std::vector<RepairPlan>& repair_plans,
          std::vector<MainRepairPlan>& main_repairs,
          std::vector<std::vector<HelpRepairPlan>>& help_repairs)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    std::vector<unsigned int> t_blocks2nodes(stripe.blocks2nodes.begin(),
        stripe.blocks2nodes.end());
    // for new locations, to optimize
    std::unordered_map<unsigned int, std::vector<unsigned int>> free_nodes_in_clusters;
    for (auto& repair_plan : repair_plans) {
      std::unordered_map<int, unsigned int> map2clusters;
      int cnt = 0;
      // 对修复方案中的可用块，统计每个集群中的可用块数量
      for (auto& help_block : repair_plan.help_blocks) {
        // 【新增检查】：防止 help_block[0] 越界崩溃
        if (help_block.empty()) {
            map2clusters[cnt++] = -1; 
            continue;
        }
        unsigned int nid = t_blocks2nodes[help_block[0]];
        unsigned int cid = node_table_[nid].map2cluster;
        map2clusters[cnt++] = cid;
      }
      // repair_plan.failure_idxs：该条带的失效块逻辑ID
      // 统计每个cluster的故障块数，选故障块最多的cluster作为主修复集群
      std::unordered_map<unsigned int, int> failures_cnt;
      unsigned int main_cid = 0;
      int max_cnt_val = 0;
      for (auto& idx : repair_plan.failure_idxs) {
        unsigned int nid = t_blocks2nodes[idx];
        unsigned int cid = node_table_[nid].map2cluster;
        if (failures_cnt.find(cid) == failures_cnt.end()) {
          failures_cnt[cid] = 1;
        } else {
          failures_cnt[cid]++; // 统计每个cluster的故障块数
        }
        // 选故障块最多的cluster作为主修复集群
        // 由于我们测试的节点故障，并且它是单个条带串行修复的，所以这里的主修复集群就是故障节点所在集群
        if (failures_cnt[cid] > max_cnt_val) { 
          max_cnt_val = failures_cnt[cid];
          main_cid = cid;
        }
      }
      std::unordered_set<unsigned int> failed_cluster_sets;
      for (auto it = repair_plan.failure_idxs.begin();
           it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = t_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        failed_cluster_sets.insert(cluster_id);
        if (free_nodes_in_clusters.find(cluster_id) ==
            free_nodes_in_clusters.end()) {
          std::vector<unsigned int> free_nodes;
          Cluster &cluster = cluster_table_[cluster_id];
          for (int i = 0; i < num_of_nodes_per_cluster_; i++) {
            free_nodes.push_back(cluster.nodes[i]);
          }
          free_nodes_in_clusters[cluster_id] = free_nodes;
        }
        auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                              free_nodes_in_clusters[cluster_id].end(), node_id);
        if (iter != free_nodes_in_clusters[cluster_id].end()) {
          free_nodes_in_clusters[cluster_id].erase(iter);
        }
      }
      MainRepairPlan main_plan;
      int clusters_num = repair_plan.help_blocks.size();
      CodingParameters cp;
      ec_schema_.ec->get_coding_parameters(cp);
      for (int i = 0; i < clusters_num; i++) {
        for (auto block_idx : repair_plan.help_blocks[i]) {
          main_plan.live_blocks_index.push_back(block_idx);
        }
        if (failed_cluster_sets.find(map2clusters[i]) != failed_cluster_sets.end()) {
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = t_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            main_plan.inner_cluster_help_blocks_info.push_back(
                std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            main_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            // for new locations
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                                  free_nodes_in_clusters[cluster_id].end(), node_id);
            if (iter != free_nodes_in_clusters[cluster_id].end()) {
              free_nodes_in_clusters[cluster_id].erase(iter);
            }
          }
        }
      }
      main_plan.ec_type = ec_schema_.ec_type;
      stripe.ec->get_coding_parameters(main_plan.cp);
      main_plan.cluster_id = main_cid;
      main_plan.cp.x = ec_schema_.x;
      main_plan.cp.seri_num = stripe_id % ec_schema_.x;
      main_plan.cp.local_or_column = repair_plan.local_or_column;
      main_plan.block_size = stripe.block_size;
      main_plan.partial_decoding = ec_schema_.partial_decoding;
      main_plan.partial_scheme = ec_schema_.partial_scheme;
      // 添加故障块信息
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        main_plan.failed_blocks_index.push_back(*it);
        main_plan.failed_block_ids.push_back(stripe.block_ids[*it]);
      }
      for (auto block_idx : repair_plan.parity_idxs) {
        main_plan.parity_blocks_index.push_back(block_idx);
      }
      std::vector<HelpRepairPlan> help_plans;
      // 添加helper块信息
      for (int i = 0; i < clusters_num; i++) {
        // 【新增检查】
        if (repair_plan.help_blocks[i].empty()) continue;
        if (map2clusters[i] != main_cid) {
          HelpRepairPlan help_plan;
          help_plan.ec_type = main_plan.ec_type;
          help_plan.cp = main_plan.cp;
          help_plan.cluster_id = map2clusters[i];
          help_plan.block_size = main_plan.block_size;
          help_plan.partial_decoding = main_plan.partial_decoding;
          help_plan.partial_scheme = main_plan.partial_scheme;
          help_plan.isvertical = main_plan.isvertical;
          if (ec_schema_.partial_scheme) {
            for(auto it = repair_plan.failure_idxs.begin();
              it != repair_plan.failure_idxs.end(); it++) {
              help_plan.failed_blocks_index.push_back(*it);
            }
          }
          for(auto it = main_plan.parity_blocks_index.begin(); 
              it != main_plan.parity_blocks_index.end(); it++) {
            help_plan.parity_blocks_index.push_back(*it);
          }
          for(auto it = main_plan.live_blocks_index.begin(); 
              it != main_plan.live_blocks_index.end(); it++) {
            help_plan.live_blocks_index.push_back(*it);
          }
          int num_of_help_blocks = 0;
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = t_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            help_plan.inner_cluster_help_blocks_info.push_back(
                std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            help_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            num_of_help_blocks++;
          }
          // 判断是否能够使用局部修复，减少数据传输
          if (ec_schema_.partial_scheme) {
            int failed_num = (int) help_plan.failed_blocks_index.size();
            if (num_of_help_blocks > failed_num) {
              help_plan.partial_less = true;
            }
          } else {
            int num_of_partial_blocks =
                ec_schema_.ec->num_of_partial_blocks_to_transfer(
                    repair_plan.help_blocks[i], help_plan.parity_blocks_index);
            if (num_of_partial_blocks < num_of_help_blocks) {
              help_plan.partial_less = true;
            }
          }
          main_plan.help_clusters_partial_less.push_back(help_plan.partial_less);
          main_plan.help_clusters_blocks_info.push_back(
              help_plan.inner_cluster_help_blocks_info);
          main_plan.help_clusters_block_ids.push_back(
              help_plan.inner_cluster_help_block_ids);
          help_plan.main_proxy_ip = cluster_table_[main_cid].proxy_ip;
          help_plan.main_proxy_port =
              cluster_table_[main_cid].proxy_port + SOCKET_PORT_OFFSET;
          help_plans.push_back(help_plan);
        }
      }
      // 选择新存储位置
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = t_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        std::vector<unsigned int> &free_nodes = free_nodes_in_clusters[cluster_id];
        int ran_node_idx = -1;
        unsigned int new_node_id = 0;
        // 从该cluster的可用节点中随机选择一个新节点
        ran_node_idx = random_index(free_nodes.size());
        new_node_id = free_nodes[ran_node_idx];
        auto iter = std::find(free_nodes.begin(), free_nodes.end(), new_node_id);
        if (iter != free_nodes.end()) {
          free_nodes.erase(iter);
        }
        t_blocks2nodes[*it] = new_node_id;
        std::string node_ip = node_table_[new_node_id].node_ip;
        int node_port = node_table_[new_node_id].node_port;
        main_plan.new_locations.push_back(
            std::make_pair(new_node_id, std::make_pair(node_ip, node_port)));
      }
      main_repairs.push_back(main_plan);
      help_repairs.push_back(help_plans);
    }
    return true;
  }

  // 为Product Code生成特殊的修复计划
  bool Coordinator::concrete_repair_plans_pc(
          int stripe_id,
          std::vector<RepairPlan>& repair_plans,
          std::vector<MainRepairPlan>& main_repairs,
          std::vector<std::vector<HelpRepairPlan>>& help_repairs)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    std::vector<unsigned int> t_blocks2nodes(stripe.blocks2nodes.begin(),
        stripe.blocks2nodes.end());
    std::unordered_map<unsigned int, std::vector<unsigned int>> free_nodes_in_clusters;
    CodingParameters cp;
    stripe.ec->get_coding_parameters(cp);
    ProductCode pc;
    pc.init_coding_parameters(cp);
    for (auto& repair_plan : repair_plans) {
      std::unordered_map<int, unsigned int> map2clusters;
      int cnt = 0;
      for (auto& help_block : repair_plan.help_blocks) {
        unsigned int nid = t_blocks2nodes[help_block[0]];
        unsigned int cid = node_table_[nid].map2cluster;
        map2clusters[cnt++] = cid;
      }
      std::unordered_map<unsigned int, int> failures_cnt;
      unsigned int main_cid = 0;
      int max_cnt_val = 0;
      for (auto& idx : repair_plan.failure_idxs) {
        unsigned int nid = t_blocks2nodes[idx];
        unsigned int cid = node_table_[nid].map2cluster;
        if (failures_cnt.find(cid) == failures_cnt.end()) {
          failures_cnt[cid] = 1;
        } else {
          failures_cnt[cid]++;
        }
        if (failures_cnt[cid] > max_cnt_val) {
          max_cnt_val = failures_cnt[cid];
          main_cid = cid;
        }
      }
      // for new locations, to optimize
      std::unordered_set<unsigned int> failed_cluster_sets;
      for (auto it = repair_plan.failure_idxs.begin();
           it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = t_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        failed_cluster_sets.insert(cluster_id);
        if (free_nodes_in_clusters.find(cluster_id) ==
            free_nodes_in_clusters.end()) {
          std::vector<unsigned int> free_nodes;
          Cluster &cluster = cluster_table_[cluster_id];
          for (int i = 0; i < num_of_nodes_per_cluster_; i++) {
            free_nodes.push_back(cluster.nodes[i]);
          }
          free_nodes_in_clusters[cluster_id] = free_nodes;
        }
        auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                              free_nodes_in_clusters[cluster_id].end(), node_id);
        if (iter != free_nodes_in_clusters[cluster_id].end()) {
          free_nodes_in_clusters[cluster_id].erase(iter);
        }
      }
      int row = -1, col = -1;
      MainRepairPlan main_plan;
      if (ec_schema_.multistripe_placement_rule == VERTICAL) {
          main_plan.isvertical = true;
      }
      int clusters_num = repair_plan.help_blocks.size();
      for (int i = 0; i < clusters_num; i++) {
        for (auto block_idx : repair_plan.help_blocks[i]) {
          pc.bid2rowcol(block_idx, row, col);
          if (repair_plan.local_or_column) {
            main_plan.live_blocks_index.push_back(row);
          } else {
            main_plan.live_blocks_index.push_back(col);
          }
        }
        if (failed_cluster_sets.find(map2clusters[i]) != failed_cluster_sets.end()) {
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = t_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            pc.bid2rowcol(block_idx, row, col);
            if (repair_plan.local_or_column) {
              main_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(row, std::make_pair(node_ip, node_port)));
            } else {
              main_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(col, std::make_pair(node_ip, node_port)));
            }

            main_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            
            // for new locations
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                                  free_nodes_in_clusters[cluster_id].end(), node_id);
            if (iter != free_nodes_in_clusters[cluster_id].end()) {
              free_nodes_in_clusters[cluster_id].erase(iter);
            }
          }
        }
      }
      
      main_plan.ec_type = RS;
      if (repair_plan.local_or_column) {
        main_plan.cp.k = pc.k2;
        main_plan.cp.m = pc.m2;
      } else {
        main_plan.cp.k = pc.k1;
        main_plan.cp.m = pc.m1;
      }
      main_plan.cluster_id = main_cid;
      main_plan.cp.x = ec_schema_.x;
      main_plan.cp.seri_num = stripe_id % ec_schema_.x;
      main_plan.cp.local_or_column = repair_plan.local_or_column;
      main_plan.block_size = stripe.block_size;
      main_plan.partial_decoding = ec_schema_.partial_decoding;
      main_plan.partial_scheme = ec_schema_.partial_scheme;
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        pc.bid2rowcol(*it, row, col);
        if (repair_plan.local_or_column) {
          main_plan.failed_blocks_index.push_back(row);
        } else {
          main_plan.failed_blocks_index.push_back(col);
        }
        main_plan.failed_block_ids.push_back(stripe.block_ids[*it]);
      }
      for (auto block_idx : repair_plan.parity_idxs) {
        pc.bid2rowcol(block_idx, row, col);
        if (repair_plan.local_or_column) {
          main_plan.parity_blocks_index.push_back(row);
        } else {
          main_plan.parity_blocks_index.push_back(col);
        }
      }
      std::vector<HelpRepairPlan> help_plans;
      for (int i = 0; i < clusters_num; i++) {
        if (map2clusters[i] != main_cid) {
          HelpRepairPlan help_plan;
          help_plan.ec_type = main_plan.ec_type;
          help_plan.cp = main_plan.cp;
          help_plan.cluster_id = map2clusters[i];
          help_plan.block_size = main_plan.block_size;
          help_plan.partial_decoding = main_plan.partial_decoding;
          help_plan.partial_scheme = main_plan.partial_scheme;
          help_plan.isvertical = main_plan.isvertical;
          if (ec_schema_.partial_scheme) {
            for(auto it = main_plan.failed_blocks_index.begin();
                it != main_plan.failed_blocks_index.end(); it++) {
              help_plan.failed_blocks_index.push_back(*it);
            }
          }
          for(auto it = main_plan.parity_blocks_index.begin(); 
              it != main_plan.parity_blocks_index.end(); it++) {
            help_plan.parity_blocks_index.push_back(*it);
          }
          for(auto it = main_plan.live_blocks_index.begin(); 
              it != main_plan.live_blocks_index.end(); it++) {
            help_plan.live_blocks_index.push_back(*it);
          }
          int num_of_help_blocks = 0;
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = t_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            pc.bid2rowcol(block_idx, row, col);
            if (repair_plan.local_or_column) {
              help_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(row, std::make_pair(node_ip, node_port)));
            } else {
              help_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(col, std::make_pair(node_ip, node_port)));
            }
            help_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            num_of_help_blocks++;
          }
          if (ec_schema_.partial_scheme) {
            int failed_num = (int) help_plan.failed_blocks_index.size();
            if (num_of_help_blocks > failed_num) {
              help_plan.partial_less = true;
            }
          } else {
            int num_of_partial_blocks =
                ec_schema_.ec->num_of_partial_blocks_to_transfer(
                    repair_plan.help_blocks[i], help_plan.parity_blocks_index);
            if (num_of_partial_blocks < num_of_help_blocks) {
              help_plan.partial_less = true;
            }
          }
          main_plan.help_clusters_partial_less.push_back(help_plan.partial_less);
          main_plan.help_clusters_blocks_info.push_back(
              help_plan.inner_cluster_help_blocks_info);
          main_plan.help_clusters_block_ids.push_back(
              help_plan.inner_cluster_help_block_ids);
          help_plan.main_proxy_ip = cluster_table_[main_cid].proxy_ip;
          help_plan.main_proxy_port =
              cluster_table_[main_cid].proxy_port + SOCKET_PORT_OFFSET;
          help_plans.push_back(help_plan);
        }
      }
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = t_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        std::vector<unsigned int> &free_nodes = free_nodes_in_clusters[cluster_id];
        int ran_node_idx = -1;
        unsigned int new_node_id = 0;
        ran_node_idx = random_index(free_nodes.size());
        new_node_id = free_nodes[ran_node_idx];
        auto iter = std::find(free_nodes.begin(), free_nodes.end(), new_node_id);
        if (iter != free_nodes.end()) {
          free_nodes.erase(iter);
        }

        t_blocks2nodes[*it] = new_node_id;
        std::string node_ip = node_table_[new_node_id].node_ip;
        int node_port = node_table_[new_node_id].node_port;
        main_plan.new_locations.push_back(
            std::make_pair(new_node_id, std::make_pair(node_ip, node_port)));
      }
      main_repairs.push_back(main_plan);
      help_repairs.push_back(help_plans);
    }
    return true;
  }

  // 模拟修复过程，计算性能指标：跨集群传输量、IO次数
  void Coordinator::simulation_repair(
          std::vector<MainRepairPlan>& main_repair,
          int& cross_cluster_transfers,
          int& io_cnt)
  {
    if (IF_DEBUG) {
      std::cout << "Simulation:\n"; 
    }
    for (int i = 0; i < int(main_repair.size()); i++) {
      int failed_num = int(main_repair[i].failed_blocks_index.size());
      for (int j = 0; j < int(main_repair[i].help_clusters_blocks_info.size()); j++) {
        int num_of_help_blocks = int(main_repair[i].help_clusters_blocks_info[j].size());
        int num_of_partial_blocks = failed_num;
        if (IF_DEBUG) {
          std::cout << "Cluster " << j << ": ";
          for (auto& kv : main_repair[i].help_clusters_blocks_info[j]) {
            std::cout << kv.first << " ";
          }
          std::cout << " | ";
        }
        if (!ec_schema_.partial_scheme) {
        std::vector<int> local_data_idxs;
          for (auto& kv : main_repair[i].help_clusters_blocks_info[j]) {
            local_data_idxs.push_back(kv.first);
          }
          ec_schema_.ec->local_or_column = main_repair[i].cp.local_or_column;
          num_of_partial_blocks =
              ec_schema_.ec->num_of_partial_blocks_to_transfer(
                  local_data_idxs, main_repair[i].parity_blocks_index);
        }
        
        if (num_of_help_blocks > num_of_partial_blocks && ec_schema_.partial_decoding) {
          cross_cluster_transfers += num_of_partial_blocks;
          if (IF_DEBUG) {
            std::cout << num_of_partial_blocks << std::endl;
          }
        } else {
          cross_cluster_transfers += num_of_help_blocks;
          if (IF_DEBUG) {
            std::cout << num_of_help_blocks << std::endl;
          }
        }
        io_cnt += num_of_help_blocks;
      }
      for (auto& kv : main_repair[i].new_locations) {
        unsigned int cid = node_table_[kv.first].map2cluster;
        if (cid != main_repair[i].cluster_id) {
          cross_cluster_transfers++;
        }
        if (IF_DEBUG) {
          std::cout << "New block will be placed at node " << kv.first
                    << " (cluster " << cid << ")" << std::endl;
        }
      }
      io_cnt += failed_num;
      io_cnt += int(main_repair[i].inner_cluster_help_block_ids.size());
    }
  }

  // 读取最小费用最大流修复方案
  bool Coordinator::loadRepairData(const std::string& filename,
                     std::vector<int>& main_help_clusterID,
                     std::vector<std::vector<std::pair<int, int>>>& other_help_clusterID_chunkNum_pairs) {
                      
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "错误: 无法打开文件 " << filename << " 进行读取" << std::endl;
        perror("具体错误");
        return false;
    }
    
    // 清空原有数据
    main_help_clusterID.clear();
    other_help_clusterID_chunkNum_pairs.clear();
    
    // 1. 读取main_help_clusterID数组
    size_t main_size = 0;
    file.read(reinterpret_cast<char*>(&main_size), sizeof(main_size));
    
    if (main_size > 0) {
        main_help_clusterID.resize(main_size);
        file.read(reinterpret_cast<char*>(main_help_clusterID.data()), 
                  main_size * sizeof(int));
    }
    
    // 2. 读取other_help_clusterID_chunkNum_pairs
    size_t outer_size = 0;
    file.read(reinterpret_cast<char*>(&outer_size), sizeof(outer_size));
    
    if (outer_size > 0) {
        other_help_clusterID_chunkNum_pairs.resize(outer_size);
        
        for (size_t i = 0; i < outer_size; ++i) {
            size_t inner_size = 0;
            file.read(reinterpret_cast<char*>(&inner_size), sizeof(inner_size));
            
            if (inner_size > 0) {
                other_help_clusterID_chunkNum_pairs[i].resize(inner_size);
                
                for (size_t j = 0; j < inner_size; ++j) {
                    int first, second;
                    file.read(reinterpret_cast<char*>(&first), sizeof(first));
                    file.read(reinterpret_cast<char*>(&second), sizeof(second));
                    other_help_clusterID_chunkNum_pairs[i][j] = {first, second};
                }
            }
        }
    }
    
    if (file.fail()) {
        std::cerr << "错误: 读取文件 " << filename << " 时发生错误" << std::endl;
        return false;
    }
    
    file.close();
    
    std::cout << "成功从 " << filename << " 读取集群数据:" << std::endl;
    std::cout << "  - main_help_clusterID 大小: " << main_help_clusterID.size() << std::endl;
    std::cout << "  - other_help_clusterID_chunkNum_pairs 大小: " 
              << other_help_clusterID_chunkNum_pairs.size() << std::endl;
    
    return true;
  }

  void Coordinator::do_flow_repair(
          std::vector<unsigned int> failed_ids, int stripe_id,
          RepairResp& response)
  {
    struct timeval start_time, end_time;
    struct timeval m_start_time, m_end_time;
    double repair_time = 0;
    double decoding_time = 0;
    double cross_cluster_time = 0;
    double meta_time = 0;
    int cross_cluster_transfers = 0;
    int io_cnt = 0;
    std::unordered_map<unsigned int, std::vector<int>> failures_map;
    // 检测故障范围
    check_out_failures(stripe_id, failed_ids, failures_map);
    // ----------------------------------------------
    // 将 failures_map 中的条带以 Available 矩阵格式输入至文件，供 min_cost_max_flow.cpp 读取生成修复方案
    
    // 获取集群数量（假设集群ID从0到num_clusters-1连续）
    int num_clusters = cluster_table_.size();
    if (num_clusters == 0) {
        std::cerr << "[ERROR] No clusters available!" << std::endl;
        return;
    }

    // 收集所有待修复的条带，按 failures_map 的遍历顺序（即当前无序，但为了保持与后续处理一致，我们显式保存顺序）
    std::vector<int> ordered_stripe_ids;
    for (const auto& pair : failures_map) {
        ordered_stripe_ids.push_back(pair.first);
    }

    // 构建 Available 矩阵：行 = 条带（按 ordered_stripe_ids 顺序），列 = 集群ID（0..num_clusters-1）
    std::vector<std::vector<int>> available_matrix;
    available_matrix.reserve(ordered_stripe_ids.size());

    for (int stripe_id : ordered_stripe_ids) {
        Stripe& stripe = stripe_table_[stripe_id];
        const auto& failed_blocks = failures_map[stripe_id];  // 故障块索引列表
        std::unordered_set<int> failed_set(failed_blocks.begin(), failed_blocks.end());

        // 初始化该条带在每个集群的可用块计数为0
        std::vector<int> cluster_counts(num_clusters, 0);

        // 遍历该条带的所有块（假设总块数为 k+m，可以从 stripe 中获取，例如 stripe.ec->k + stripe.ec->m）
        // int total_blocks = stripe.ec->k + stripe.ec->m;
        // 【修改后】使用真实的 blocks2nodes 大小防止越界读取垃圾数据
        int total_blocks = stripe.blocks2nodes.size();
        for (int block_idx = 0; block_idx < total_blocks; ++block_idx) {
            if (failed_set.find(block_idx) != failed_set.end()) {
                continue;  // 故障块不计入可用
            }
            // 获取块所在的节点ID，再映射到集群ID
            unsigned int node_id = stripe.blocks2nodes[block_idx];
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            // 确保 cluster_id 在合法范围内
            if (cluster_id >= 0 && cluster_id < num_clusters) {
                cluster_counts[cluster_id]++;
            } else {
                std::cerr << "[WARNING] Block " << block_idx << " of stripe " << stripe_id
                          << " is on invalid cluster " << cluster_id << std::endl;
            }
        }
        available_matrix.push_back(cluster_counts);
    }

    // 将矩阵写入文件 "../Available_matrix"
    std::string filename = "/home/hadoop/zzy/ec_prototype-master/flow_repair/Available_matrix";
    std::ofstream outfile(filename);
    if (!outfile.is_open()) {
        std::cerr << "[ERROR] Cannot create Available matrix file: " << filename << std::endl;
        return;
    }

    outfile << "Available = {\n";
    for (size_t i = 0; i < available_matrix.size(); ++i) {
        outfile << "    {";
        const auto& row = available_matrix[i];
        for (size_t j = 0; j < row.size(); ++j) {
            outfile << row[j];
            if (j < row.size() - 1) outfile << ",";
        }
        outfile << "}";
        if (i < available_matrix.size() - 1) outfile << ",";
        outfile << "\n";
    }
    outfile << "};\n";
    outfile.close();

    std::cout << "[INFO] Available matrix written to " << filename << " with "
              << available_matrix.size() << " rows." << std::endl;

    // 自动调用 min_cost_max_flow 程序生成 cluster_data.bin
    // 编译后的可执行文件名为 "min_cost_max_flow"
    std::string command = "/home/hadoop/zzy/ec_prototype-master/flow_repair/complete_min_cost_max_flow";
    int ret = system(command.c_str());
    if (ret != 0) {
        std::cerr << "[ERROR] Failed to execute min_cost_max_flow. Return code: " << ret << std::endl;
        std::cerr << "Please ensure the program is compiled and available in the current directory." << std::endl;
        return;
    }
    std::cout << "[INFO] min_cost_max_flow executed successfully." << std::endl;


    // ----------------------------------------------
    // 读取最小费用最大流算法生成的修复方案
    std::vector<int> main_help_clusterID;
    std::vector<std::vector<std::pair<int, int>>> other_help_clusterID_chunkNum_pairs;
    if (!loadRepairData("/home/hadoop/zzy/ec_prototype-master/flow_repair/cluster_data.bin", main_help_clusterID, other_help_clusterID_chunkNum_pairs)) {
        std::cerr << "[ERROR]: Read cluster_data.bin error! " << std::endl;
        return;
    }
    // 复制一份 main_help_clusterID，因为后面会修改为 分区ID 的映射关系
    std::vector<int> main_help_clusterID_original = main_help_clusterID;
    
    // 验证数据是否正确加载
    if (IF_DEBUG) {
      std::cout << "\n[Check]: element in main_help_clusterID : " << std::endl;
      for (size_t i = 0; i < main_help_clusterID.size(); ++i) {
          std::cout << main_help_clusterID[i] << " ";
      }
      std::cout << std::endl;
      
      std::cout << "[Check]: first vector size in other_help_clusterID_chunkNum_pairs : " << std::endl;
      for (size_t i = 0; i < other_help_clusterID_chunkNum_pairs.size(); ++i) {
        std::cout << "[Check]: Stripe " << i << " 's {help_cluster, chunk_num} vector: " ;
        
        for (size_t j = 0; j < other_help_clusterID_chunkNum_pairs[i].size(); ++j) {
            const auto& p = other_help_clusterID_chunkNum_pairs[i][j];
            std::cout << "(" << p.first << ", " << p.second << ") ";
        }
        
        if (other_help_clusterID_chunkNum_pairs[i].empty()) {
            std::cout << "null";
        }
        std::cout << std::endl;
      }
    }

    // -------------------------------------------------
    int failure_stripe_ID = 0;
    for (auto& pair : failures_map) {
      gettimeofday(&start_time, NULL);
      gettimeofday(&m_start_time, NULL);
      Stripe& stripe = stripe_table_[pair.first];
      stripe.ec->placement_rule = ec_schema_.placement_rule;
      stripe.ec->generate_partition();
      find_out_stripe_partitions(pair.first);

      bool main_help_cluster_flag = false;   // 新增变量：用于处理目的机架无可用块的情况
      
      std::cout << "Stripe " << stripe.stripe_id << " block placement:\n";


      int target_des_cluster = main_help_clusterID[failure_stripe_ID];
      auto& other_vec = other_help_clusterID_chunkNum_pairs[failure_stripe_ID];
      std::unordered_map<int, size_t> cluster_to_idx;
      for (size_t idx = 0; idx < other_vec.size(); ++idx) {
        cluster_to_idx[other_vec[idx].first] = idx;
      }

      // 新增变量分区ID
      unsigned int partition_id = 0;
      int helper_cluster_find_num = 0;

      for (auto& vec : stripe.ec->partition_plan) {
        unsigned int node_id = stripe.blocks2nodes[vec[0]];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        std::cout << cluster_id << ": ";
        for (int ele : vec) {
          std::cout << "B" << ele << "N" << stripe.blocks2nodes[ele] << " ";
        }
        // 1. 检查是否为主帮助集群（目的机架）
        // 如果找到了目的机架ID
        if (cluster_id == target_des_cluster) {
          std::cout << "  --> des_rack ";
          // 将机架ID转为分区ID
          main_help_clusterID[failure_stripe_ID] = partition_id;
          main_help_cluster_flag = true;

          // 如果该集群同时也出现在 other 列表中，则一并处理（避免遗漏）
          auto it = cluster_to_idx.find(cluster_id);
          if (it != cluster_to_idx.end()) {
              std::cout << "  --> other_helper ";
              other_vec[it->second].first = partition_id;  // 转换为分区ID
              helper_cluster_find_num++;
              cluster_to_idx.erase(it);  // 防止重复匹配
          }
        } else {
          auto it = cluster_to_idx.find(cluster_id);
          if (it != cluster_to_idx.end()) {
              std::cout << "  --> other_helper ";
              other_vec[it->second].first = partition_id;
              helper_cluster_find_num++;
              cluster_to_idx.erase(it);
          }
        }
        std::cout << "\n";
        partition_id++;
      }
      // 验证是否所有 other 帮助集群都已找到
      if (helper_cluster_find_num != other_vec.size()) {
          std::cout << "[ERROR] : other_help_clusterID_chunkNum_pairs not found all! "
                    << "Found " << helper_cluster_find_num
                    << ", expected " << other_vec.size() << std::endl;
      }
      // 如果目的机架中没有可用块，即找不到对应分区ID
      if (!main_help_cluster_flag) {
          std::cout << "[NOTE] : main_help_cluster has no chunk" << std::endl;
      }
      
      std::cout << "Generating repair plan for failures:" << std::endl;
      for (auto& failure : pair.second) {
        std::cout << failure << " ";
      }
      std::cout << std::endl;
      
      std::vector<RepairPlan> repair_plans;

      // 生成修复方案
      
      bool flag = stripe.ec->generate_flow_repair_plan(pair.second, repair_plans,
                                                  ec_schema_.partial_scheme,
                                                  ec_schema_.repair_priority,
                                                  ec_schema_.repair_method,
                                                  main_help_clusterID,
                                                  other_help_clusterID_chunkNum_pairs,
                                                  failure_stripe_ID,
                                                  main_help_cluster_flag);
      if (!flag) {
        response.success = false;
        return;
      }
      if (IF_DEBUG) {
        std::cout << "Repair Plan: " << std::endl;
        for (int i = 0; i < int(repair_plans.size()); i++) {
          RepairPlan& tmp = repair_plans[i];
          std::cout << "> Failed Blocks: ";
          for (int j = 0; 
               j < int(tmp.failure_idxs.size()); j++) {
            std::cout << tmp.failure_idxs[j] << " ";
          }
          std::cout << std::endl;
          std::cout << "> Repair by Blocks: ";
          for (auto& help_blocks : tmp.help_blocks) {
            for(auto& block : help_blocks) {
              std::cout << block << " ";
            }
          }
          std::cout << std::endl;
          std::cout << "> local_or_column: " << tmp.local_or_column << std::endl;
          std::cout << "> Parity idx: ";
          for (auto& idx : tmp.parity_idxs) {
            std::cout << idx << " ";
          }
          std::cout << std::endl;
        }
      }
      std::vector<MainRepairPlan> main_repairs;
      std::vector<std::vector<HelpRepairPlan>> help_repairs;
      // 生成具体的修复方案
      if (check_ec_family(ec_schema_.ec_type) == PCs) { // 乘积码
        concrete_repair_plans_pc(pair.first, repair_plans, main_repairs, help_repairs);
      } else { // 通用处理
        concrete_flow_repair_plans(pair.first, repair_plans, main_repairs, help_repairs, main_help_clusterID_original[failure_stripe_ID]);
      }
      
      if (IF_DEBUG) {
        std::cout << "Finish generate repair plan." << std::endl;
      }

      auto lock_ptr = std::make_shared<std::mutex>();

      auto send_main_repair_plan = 
          [this, main_repairs, lock_ptr, &decoding_time, &cross_cluster_time](
              int i, int main_cluster_id) mutable
      {
        std::string chosen_proxy = cluster_table_[main_cluster_id].proxy_ip +
            std::to_string(cluster_table_[main_cluster_id].proxy_port);
        // 向主修复proxy发送修复任务，主修复proxy指故障块最多的集群，负责解码计算
        auto resp = 
            async_simple::coro::syncAwait(proxies_[chosen_proxy]
                ->call_for<&Proxy::main_repair>(
                    std::chrono::seconds{500}, main_repairs[i])).value();
        lock_ptr->lock();
        decoding_time += resp.decoding_time;
        cross_cluster_time += resp.cross_cluster_time;
        lock_ptr->unlock();
        if (IF_DEBUG) {
          std::cout << "Selected main proxy " << chosen_proxy << " of cluster"
                    << main_cluster_id << ". Decoding time : " 
                    << decoding_time << std::endl;
        }
      };

      auto send_help_repair_plan = 
          [this, help_repairs](
              int i, int j, std::string proxy_ip, int proxy_port) mutable
      {
        std::string chosen_proxy = proxy_ip + std::to_string(proxy_port);
        async_simple::coro::syncAwait(proxies_[chosen_proxy]
            ->call_for<&Proxy::help_repair>(std::chrono::seconds{500}, help_repairs[i][j]));
        if (IF_DEBUG) {
          std::cout << "Selected help proxy " << chosen_proxy << std::endl;
        }
      };

      // simulation-统计理论上的跨集群传输量、IO次数
      simulation_repair(main_repairs, cross_cluster_transfers, io_cnt);
      if (IF_DEBUG) {
        std::cout << "Finish simulation! " << cross_cluster_transfers << std::endl;
      }
      gettimeofday(&m_end_time, NULL);
      meta_time += m_end_time.tv_sec - m_start_time.tv_sec +
          (m_end_time.tv_usec - m_start_time.tv_usec) * 1.0 / 1000000;

      if (!IF_SIMULATION) {
        for (int i = 0; i < int(main_repairs.size()); i++) {
          try
          {
            MainRepairPlan& tmp = main_repairs[i];
            int failed_num = int(tmp.failed_blocks_index.size());
            unsigned int main_cluster_id = tmp.cluster_id;
            // 多线程发送修复任务
            std::thread my_main_thread(send_main_repair_plan, i, main_cluster_id);
            std::vector<std::thread> senders;
            int index = 0;
            for (int j = 0; j < int(tmp.help_clusters_blocks_info.size()); j++) {
              int num_of_blocks_in_help_cluster = 
                  (int)tmp.help_clusters_blocks_info[j].size();
              my_assert(num_of_blocks_in_help_cluster == 
                  int(help_repairs[i][j].inner_cluster_help_blocks_info.size()));
              bool t_flag = tmp.help_clusters_partial_less[j];
              if ((IF_DIRECT_FROM_NODE && ec_schema_.partial_decoding
                   && t_flag) ||
                  !IF_DIRECT_FROM_NODE)
              {
                Cluster &cluster = cluster_table_[help_repairs[i][j].cluster_id];
                senders.push_back(std::thread(send_help_repair_plan, i, j,
                                  cluster.proxy_ip, cluster.proxy_port));
              }
            }
            for (int j = 0; j < int(senders.size()); j++) {
              senders[j].join();
            }
            my_main_thread.join();
          }
          catch(const std::exception& e)
          {
            std::cerr << e.what() << '\n';
          }
        }
      }
      // 更新元数据
      for (int i = 0; i < int(main_repairs.size()); i++) {
        MainRepairPlan& tmp = main_repairs[i];
        int j = 0;
        for (auto& idx : tmp.failed_blocks_index) {
          stripe.blocks2nodes[idx] = tmp.new_locations[j++].first;
        }
      }
      gettimeofday(&end_time, NULL);
      double temp_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      repair_time += temp_time;

      std::cout << "Repair[ ";
      for (auto& failure : pair.second) {
        std::cout << failure << " ";
      }
      std::cout << "]: total = " << repair_time << "s, latest = "
                << temp_time << "s. Decode: total = "
                << decoding_time << std::endl;
      // -----------------------------------------------------
      // 新增变量以记录当前failures_map条带ID
      failure_stripe_ID++;
    }
    response.decoding_time = decoding_time;
    response.cross_cluster_time = cross_cluster_time;
    response.repair_time = repair_time;
    response.meta_time = meta_time;
    response.cross_cluster_transfers = cross_cluster_transfers;
    response.io_cnt = io_cnt;
    response.success = true;
  }

  // 为RS码、LRC码生成具体的修复执行计划
  bool Coordinator::concrete_flow_repair_plans(
          int stripe_id,
          std::vector<RepairPlan>& repair_plans,
          std::vector<MainRepairPlan>& main_repairs,
          std::vector<std::vector<HelpRepairPlan>>& help_repairs,
          unsigned int flow_main_cluster_id)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    std::vector<unsigned int> t_blocks2nodes(stripe.blocks2nodes.begin(),
        stripe.blocks2nodes.end());
    // for new locations, to optimize
    std::unordered_map<unsigned int, std::vector<unsigned int>> free_nodes_in_clusters;
    for (auto& repair_plan : repair_plans) {
      std::unordered_map<int, unsigned int> map2clusters;
      int cnt = 0;
      // 对修复方案中的可用块，统计每个集群中的可用块数量
      for (auto& help_block : repair_plan.help_blocks) {
        // 【新增检查】：如果 EC 算法返回了空的帮手块分组，直接用 -1 占位并跳过，防止 help_block[0] 越界崩溃！
        if (help_block.empty()) {
            map2clusters[cnt++] = -1; 
            continue;
        }
        unsigned int nid = t_blocks2nodes[help_block[0]];
        unsigned int cid = node_table_[nid].map2cluster;
        map2clusters[cnt++] = cid;
      }
      // repair_plan.failure_idxs：该条带的失效块逻辑ID
      // ----------------------------------------------------------
      // 根据传入的参数选择主修复集群
      unsigned int main_cid = flow_main_cluster_id;
      bool found = false;
      for (int i = 0; i < (int)map2clusters.size(); ++i) {
        if (map2clusters[i] == main_cid) {
          found = true;
          break;
        }
      }
      if (!found) {
        std::cerr << "[WARNING] Main cluster ID " << main_cid
                  << " not found in help block groups, using first group as fallback." << std::endl;
        main_cid = map2clusters[0];  // 回退到第一个分组
      }

      // ============================================================
      // 【修改一】提前完整初始化主集群空闲节点列表
      // 原逻辑在 failed_cluster_sets 循环内懒初始化，导致：
      //   1. 若失效块不在主集群则主集群节点列表为空
      //   2. 活跃块占用的节点未从空闲列表剔除
      // 现改为：在确定 main_cid 后立即初始化，并排除所有活跃块节点
      // ============================================================
      if (free_nodes_in_clusters.find(main_cid) == free_nodes_in_clusters.end()) {
        std::vector<unsigned int> free_nodes;
        Cluster& cluster = cluster_table_[main_cid];
        for (int i = 0; i < num_of_nodes_per_cluster_; i++) {
          free_nodes.push_back(cluster.nodes[i]);
        }
        free_nodes_in_clusters[main_cid] = free_nodes;
      }
      // 【修改一续】遍历所有帮助块，将主集群内活跃块占用的节点从空闲列表中移除
      for (auto& help_block_group : repair_plan.help_blocks) {
        for (auto block_idx : help_block_group) {
          unsigned int nid = t_blocks2nodes[block_idx];
          if (node_table_[nid].map2cluster == main_cid) {
            auto& fn = free_nodes_in_clusters[main_cid];
            auto it = std::find(fn.begin(), fn.end(), nid);
            if (it != fn.end()) fn.erase(it);
          }
        }
      }
      // ============================================================

      std::unordered_set<unsigned int> failed_cluster_sets;
      for (auto it = repair_plan.failure_idxs.begin();
           it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = t_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        failed_cluster_sets.insert(cluster_id);
        // ============================================================
        // 【修改二】删除此处原有的懒初始化逻辑
        // 原代码在此处初始化 free_nodes_in_clusters[main_cid] 并仅剔除失效块节点，
        // 已由【修改一】提前完整处理，此处不再需要，予以删除。
        // 原代码（已删除）：
        //   if (free_nodes_in_clusters.find(main_cid) == free_nodes_in_clusters.end()) {
        //     std::vector<unsigned int> free_nodes;
        //     Cluster &cluster = cluster_table_[main_cid];
        //     for (int i = 0; i < num_of_nodes_per_cluster_; i++) {
        //       free_nodes.push_back(cluster.nodes[i]);
        //     }
        //     free_nodes_in_clusters[main_cid] = free_nodes;
        //   }
        //   auto iter = std::find(free_nodes_in_clusters[main_cid].begin(),
        //                         free_nodes_in_clusters[main_cid].end(), node_id);
        //   if (iter != free_nodes_in_clusters[main_cid].end()) {
        //     free_nodes_in_clusters[main_cid].erase(iter);
        //   }
        // ============================================================
      }
      MainRepairPlan main_plan;
      int clusters_num = repair_plan.help_blocks.size();
      CodingParameters cp;
      ec_schema_.ec->get_coding_parameters(cp);
      for (int i = 0; i < clusters_num; i++) {
        for (auto block_idx : repair_plan.help_blocks[i]) {
          main_plan.live_blocks_index.push_back(block_idx);
        }
        if (failed_cluster_sets.find(map2clusters[i]) != failed_cluster_sets.end()) {
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = t_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            main_plan.inner_cluster_help_blocks_info.push_back(
                std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            main_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            // for new locations
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                                  free_nodes_in_clusters[cluster_id].end(), node_id);
            if (iter != free_nodes_in_clusters[cluster_id].end()) {
              free_nodes_in_clusters[cluster_id].erase(iter);
            }
          }
        }
      }
      main_plan.ec_type = ec_schema_.ec_type;
      stripe.ec->get_coding_parameters(main_plan.cp);
      main_plan.cluster_id = main_cid;
      main_plan.cp.x = ec_schema_.x;
      main_plan.cp.seri_num = stripe_id % ec_schema_.x;
      main_plan.cp.local_or_column = repair_plan.local_or_column;
      main_plan.block_size = stripe.block_size;
      main_plan.partial_decoding = ec_schema_.partial_decoding;
      main_plan.partial_scheme = ec_schema_.partial_scheme;
      // 添加故障块信息
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        main_plan.failed_blocks_index.push_back(*it);
        main_plan.failed_block_ids.push_back(stripe.block_ids[*it]);
      }
      for (auto block_idx : repair_plan.parity_idxs) {
        main_plan.parity_blocks_index.push_back(block_idx);
      }
      std::vector<HelpRepairPlan> help_plans;
      // 添加helper块信息
      for (int i = 0; i < clusters_num; i++) {
        // 【新增检查】：遇到空分组直接跳过，防止发送无效 RPC
        if (repair_plan.help_blocks[i].empty()) continue;
        if (map2clusters[i] != main_cid) {
          HelpRepairPlan help_plan;
          help_plan.ec_type = main_plan.ec_type;
          help_plan.cp = main_plan.cp;
          help_plan.cluster_id = map2clusters[i];
          help_plan.block_size = main_plan.block_size;
          help_plan.partial_decoding = main_plan.partial_decoding;
          help_plan.partial_scheme = main_plan.partial_scheme;
          help_plan.isvertical = main_plan.isvertical;
          if (ec_schema_.partial_scheme) {
            for(auto it = repair_plan.failure_idxs.begin();
              it != repair_plan.failure_idxs.end(); it++) {
              help_plan.failed_blocks_index.push_back(*it);
            }
          }
          for(auto it = main_plan.parity_blocks_index.begin(); 
              it != main_plan.parity_blocks_index.end(); it++) {
            help_plan.parity_blocks_index.push_back(*it);
          }
          for(auto it = main_plan.live_blocks_index.begin(); 
              it != main_plan.live_blocks_index.end(); it++) {
            help_plan.live_blocks_index.push_back(*it);
          }
          int num_of_help_blocks = 0;
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = t_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            help_plan.inner_cluster_help_blocks_info.push_back(
                std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            help_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            num_of_help_blocks++;
          }
          // 判断是否能够使用局部修复，减少数据传输
          if (ec_schema_.partial_scheme) {
            int failed_num = (int) help_plan.failed_blocks_index.size();
            if (num_of_help_blocks > failed_num) {
              help_plan.partial_less = true;
            }
          } else {
            int num_of_partial_blocks =
                ec_schema_.ec->num_of_partial_blocks_to_transfer(
                    repair_plan.help_blocks[i], help_plan.parity_blocks_index);
            if (num_of_partial_blocks < num_of_help_blocks) {
              help_plan.partial_less = true;
            }
          }
          main_plan.help_clusters_partial_less.push_back(help_plan.partial_less);
          main_plan.help_clusters_blocks_info.push_back(
              help_plan.inner_cluster_help_blocks_info);
          main_plan.help_clusters_block_ids.push_back(
              help_plan.inner_cluster_help_block_ids);
          help_plan.main_proxy_ip = cluster_table_[main_cid].proxy_ip;
          help_plan.main_proxy_port =
              cluster_table_[main_cid].proxy_port + SOCKET_PORT_OFFSET;
          help_plans.push_back(help_plan);
        }
      }
      // 选择新存储位置
      // ============================================================
      // 【修改三】new_locations 统一从主集群（main_cid）选取空闲节点
      // 原逻辑中 cluster_id 取自失效块原所在集群，导致修复后新块留在原集群。
      // 现直接使用 main_cid，确保所有修复块都写入主集群（des_rack）。
      // ============================================================
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        // 【修改三】cluster_id 固定为 main_cid，不再取失效块原所在集群
        // unsigned int node_id = t_blocks2nodes[*it];
        unsigned int cluster_id = main_cid;
        std::vector<unsigned int> &free_nodes = free_nodes_in_clusters[cluster_id];
        if (free_nodes.empty()) {
          std::cerr << "[ERROR] No free nodes in main cluster " << main_cid << std::endl;
          return false;
        }
        // 从该cluster的可用节点中随机选择一个新节点
        int ran_node_idx = random_index(free_nodes.size());
        unsigned int new_node_id = free_nodes[ran_node_idx];
        auto iter = std::find(free_nodes.begin(), free_nodes.end(), new_node_id);
        if (iter != free_nodes.end()) {
          free_nodes.erase(iter);
        }
        t_blocks2nodes[*it] = new_node_id;
        std::string node_ip = node_table_[new_node_id].node_ip;
        int node_port = node_table_[new_node_id].node_port;
        main_plan.new_locations.push_back(
            std::make_pair(new_node_id, std::make_pair(node_ip, node_port)));
      }
      main_repairs.push_back(main_plan);
      help_repairs.push_back(help_plans);
    }
    return true;
  }
}