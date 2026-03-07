#include "proxy.h"

namespace ECProject
{
  MainRepairResp Proxy::main_repair(MainRepairPlan repair_plan)
  {
    struct timeval start_time, end_time;
    double decoding_time = 0;
    double cross_cluster_time = 0;

    bool if_partial_decoding = repair_plan.partial_decoding;
    bool partial_scheme = repair_plan.partial_scheme;
    size_t block_size = repair_plan.block_size;
    auto ec = ec_factory(repair_plan.ec_type, repair_plan.cp);
    ec->init_coding_parameters(repair_plan.cp);
    int failed_num = (int)repair_plan.failed_blocks_index.size();
    int *erasures = new int[failed_num + 1];
    for (int i = 0; i < failed_num; i++) {
      erasures[i] = repair_plan.failed_blocks_index[i];
    }
    erasures[failed_num] = -1;

    if (IF_DEBUG) {
      std::cout << "[Main Proxy " << self_cluster_id_ << "] To repair ";
      for (int i = 0; i < failed_num; i++) {
        std::cout << erasures[i] << " ";
      }
      std::cout << std::endl;
    }

    auto live_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.live_blocks_index);
    auto fls_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.failed_blocks_index);
    auto parity_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.parity_blocks_index);
    int parity_num = (int)parity_idx_ptr->size();

    auto original_lock_ptr = std::make_shared<std::mutex>();
    auto original_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto original_blocks_idx_ptr = std::make_shared<std::vector<int>>();

    auto get_from_node = [this, original_lock_ptr, original_blocks_ptr,
                          original_blocks_idx_ptr, block_size](
                          int block_idx, unsigned int block_id,
                          std::string node_ip, int node_port) mutable
    {
      std::string block_id_str = std::to_string(block_id);
      std::vector<char> tmp_val(block_size);
      bool res = read_from_datanode(block_id_str.c_str(), block_id_str.size(), 
                         tmp_val.data(), block_size, node_ip.c_str(), node_port);
      if (!res) {
        pthread_exit(NULL);
      }
      original_lock_ptr->lock();
      original_blocks_ptr->push_back(tmp_val);
      original_blocks_idx_ptr->push_back(block_idx);
      original_lock_ptr->unlock();
    };

    auto partial_lock_ptr = std::make_shared<std::mutex>();
    auto partial_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto partial_blocks_idx_ptr = std::make_shared<std::vector<int>>();
    auto decoding_time_ptr = std::make_shared<std::vector<double>>();

    auto get_from_proxy = [this, original_lock_ptr, original_blocks_ptr,
                           original_blocks_idx_ptr, partial_lock_ptr,
                           partial_blocks_ptr, partial_blocks_idx_ptr,
                           block_size, decoding_time_ptr]
                           (std::shared_ptr<asio::ip::tcp::socket> socket_ptr) mutable
    {
      asio::error_code error;
      std::vector<unsigned char> int_buf(sizeof(int));
      asio::read(*socket_ptr, asio::buffer(int_buf, int_buf.size()), error);
      int t_cluster_id = ECProject::bytes_to_int(int_buf);
      std::vector<unsigned char> int_flag_buf(sizeof(int));
      asio::read(*socket_ptr, asio::buffer(int_flag_buf, int_flag_buf.size()), error);
      int t_flag = ECProject::bytes_to_int(int_flag_buf);
      std::string msg = "data";
      if(t_flag)
        msg = "partial";
      if (IF_DEBUG) {
        std::cout << "[Main Proxy " << self_cluster_id_ << "] Try to get "
                  << msg << " blocks from the proxy in cluster " << t_cluster_id
                  << ". " << std::endl;
      }
      if(t_flag) {  // receive partial blocks from helper proxies
        std::vector<unsigned char> num_buf(sizeof(int));
        asio::read(*socket_ptr, asio::buffer(num_buf, num_buf.size()), error);
        int partial_block_num = ECProject::bytes_to_int(num_buf);
        partial_lock_ptr->lock();
        for (int i = 0; i < partial_block_num; i++) {
          std::vector<unsigned char> block_idx_buf(sizeof(int));
          asio::read(*socket_ptr, asio::buffer(block_idx_buf,
              block_idx_buf.size()), error);
          int block_idx = ECProject::bytes_to_int(block_idx_buf);
          partial_blocks_idx_ptr->push_back(block_idx);
          std::vector<char> tmp_val(block_size);
          asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), error);
          partial_blocks_ptr->push_back(tmp_val);
        }
        partial_lock_ptr->unlock();
      } else {  // receive data blocks from help proxies
        std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
        asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks,
            int_buf_num_of_blocks.size()), error);
        int block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
        for (int i = 0; i < block_num; i++) {
          std::vector<char> tmp_val(block_size);
          std::vector<unsigned char> block_idx_buf(sizeof(int));
          asio::read(*socket_ptr, asio::buffer(block_idx_buf,
              block_idx_buf.size()), error);
          int block_idx = ECProject::bytes_to_int(block_idx_buf);
          asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), error);
          original_lock_ptr->lock();
          original_blocks_ptr->push_back(tmp_val);
          original_blocks_idx_ptr->push_back(block_idx);
          original_lock_ptr->unlock();
        }
        std::vector<unsigned char> decoding_time_buf(sizeof(double));
        asio::read(*socket_ptr, asio::buffer(decoding_time_buf,
                                        decoding_time_buf.size()));
        double temp_decoding_time = bytes_to_double(decoding_time_buf);
        decoding_time_ptr->push_back(temp_decoding_time);
      }

      if (IF_DEBUG){
        std::cout << "[Main Proxy " << self_cluster_id_
                  << "] Finish getting data from the proxy in cluster "
                  << t_cluster_id << std::endl;
      }
    };

    auto send_to_datanode = [this, block_size](
        unsigned int block_id, char *data, std::string node_ip, int node_port)
    {
      std::string block_id_str = std::to_string(block_id);
      write_to_datanode(block_id_str.c_str(), block_id_str.size(),
                        data, block_size, node_ip.c_str(), node_port);
    };

    // get original blocks inside cluster
    int num_of_original_blocks =
        (int)repair_plan.inner_cluster_help_blocks_info.size();
    if (num_of_original_blocks > 0) {
      std::vector<std::thread> readers;
      for (int i = 0; i < num_of_original_blocks; i++) {
        readers.push_back(std::thread(get_from_node,
            repair_plan.inner_cluster_help_blocks_info[i].first,
            repair_plan.inner_cluster_help_block_ids[i],
            repair_plan.inner_cluster_help_blocks_info[i].second.first,
            repair_plan.inner_cluster_help_blocks_info[i].second.second));
      }
      for (int i = 0; i < num_of_original_blocks; i++) {
        readers[i].join();
      }

      if (IF_DEBUG) {
        std::cout << "[Main Proxy " << self_cluster_id_
                  << "] Finish getting " << num_of_original_blocks
                  << " blocks inside main cluster." << std::endl;
      }
    }

    gettimeofday(&start_time, NULL);
    // get blocks or partial blocks from other clusters
    int num_of_help_clusters = (int)repair_plan.help_clusters_blocks_info.size();
    int partial_cnt = 0;  // num of partial-block sets
    if (num_of_help_clusters > 0) {
      std::vector<std::thread> readers;
      for (int i = 0; i < num_of_help_clusters; i++) {
        int num_of_blocks_in_cluster =
            (int)repair_plan.help_clusters_blocks_info[i].size();
        bool t_flag = repair_plan.help_clusters_partial_less[i];
        t_flag = (if_partial_decoding && t_flag);
        if(!t_flag && IF_DIRECT_FROM_NODE) {  // transfer blocks directly
          num_of_original_blocks += num_of_blocks_in_cluster;
          for (int j = 0; j < num_of_blocks_in_cluster; j++) {
            readers.push_back(std::thread(get_from_node,
                repair_plan.help_clusters_blocks_info[i][j].first, 
                repair_plan.help_clusters_block_ids[i][j],
                repair_plan.help_clusters_blocks_info[i][j].second.first,
                repair_plan.help_clusters_blocks_info[i][j].second.second));
          }
        } else {  // transfer blocks through proxies
          if(t_flag) { // encode partial blocks and transfer through proxies
            partial_cnt++;
          } else {   // direct transfer original blocks through proxies
            num_of_original_blocks += num_of_blocks_in_cluster;
          }
          std::shared_ptr<asio::ip::tcp::socket>
              socket_ptr = std::make_shared<asio::ip::tcp::socket>(io_context_);
          acceptor_.accept(*socket_ptr);
          readers.push_back(std::thread(get_from_proxy, socket_ptr));
        }
      }
      int num_of_readers = (int)readers.size();
      for (int i = 0; i < num_of_readers; i++) {
        readers[i].join();
      }

      // simulate cross-cluster transfer
      if (IF_SIMULATE_CROSS_CLUSTER) {
        int cross_cluster_num = num_of_original_blocks - 
            (int)repair_plan.inner_cluster_help_blocks_info.size();
        cross_cluster_num += (int)partial_blocks_idx_ptr->size();
        size_t t_val_len = (int)block_size * cross_cluster_num;
        std::string t_value = generate_random_string((int)t_val_len);
        transfer_to_networkcore(t_value.c_str(), t_val_len);
      }

      if (decoding_time_ptr->size() > 0) {
        auto max_decode = std::max_element(decoding_time_ptr->begin(),
            decoding_time_ptr->end());
        decoding_time += *max_decode;
      }
    }
    gettimeofday(&end_time, NULL);
    cross_cluster_time += end_time.tv_sec - start_time.tv_sec +
        (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    
    std::cout << "[Main Proxy " << self_cluster_id_ << "] Finish getting blocks from "
              << num_of_help_clusters << " help clusters." << std::endl;
    
    my_assert(num_of_original_blocks == (int)original_blocks_ptr->size());
    // 
    if (num_of_original_blocks > 0 && if_partial_decoding) {  // encode-and-transfer
      int partial_num = parity_num;
      if (partial_scheme) {
        partial_num = failed_num;
      }
      my_assert(partial_num > 0);
      std::vector<char *> v_data(num_of_original_blocks);
      std::vector<char *> v_coding(partial_num);
      char **data = (char **)v_data.data();
      char **coding = (char **)v_coding.data();
      for (int j = 0; j < num_of_original_blocks; j++) {
        data[j] = (*original_blocks_ptr)[j].data();
      }
      std::vector<std::vector<char>>
          v_coding_area(partial_num, std::vector<char>(block_size));
      for (int j = 0; j < partial_num; j++) {
        coding[j] = v_coding_area[j].data();
      }
      
      auto partial_flags = std::vector<bool>(partial_num, true);

      gettimeofday(&start_time, NULL);
      if (partial_scheme) {
        my_assert(parity_idx_ptr->size() > 0);
      }
      ec->encode_partial_blocks(data, coding, block_size,
          *original_blocks_idx_ptr, *parity_idx_ptr,
          *fls_idx_ptr, *live_idx_ptr, partial_flags, partial_scheme);
      gettimeofday(&end_time, NULL);
      decoding_time += end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      partial_lock_ptr->lock();
      for (int j = 0; j < partial_num; j++) {
        if (partial_flags[j]) {
          if (partial_scheme) {
            partial_blocks_idx_ptr->push_back((*fls_idx_ptr)[j]);
          } else {
            partial_blocks_idx_ptr->push_back((*parity_idx_ptr)[j]);
          }
          partial_blocks_ptr->push_back(v_coding_area[j]);
        }
      }
      partial_lock_ptr->unlock();
      partial_cnt++;
    }

    std::vector<char *> v_failures(failed_num);
    char **failures = (char **)v_failures.data();
    std::vector<std::vector<char>> v_failures_area(failed_num, std::vector<char>(block_size));
    for (int i = 0; i < failed_num; i++) {
      failures[i] = v_failures_area[i].data();
    }
    int num_of_partial_blocks = (int)partial_blocks_ptr->size();
    if (num_of_partial_blocks > 0) {
      if (IF_DEBUG) {
        std::cout << "[Main Proxy " << self_cluster_id_
                  << "] Ready to perform addition with "
                  << num_of_partial_blocks << " partial blocks!\n";
      }
      std::vector<char *> v_data(num_of_partial_blocks);
      char **data = (char **)v_data.data();
      for (int j = 0; j < num_of_partial_blocks; j++) {
        data[j] = (*partial_blocks_ptr)[j].data();
      }

      if (partial_scheme) {
        gettimeofday(&start_time, NULL);
        ec->perform_addition(data, failures, block_size,
            *partial_blocks_idx_ptr, *fls_idx_ptr);
        gettimeofday(&end_time, NULL);
        decoding_time += end_time.tv_sec - start_time.tv_sec +
            (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      } else {
        std::vector<char *> v_coding(parity_num);
        char **coding = (char **)v_coding.data();
        std::vector<std::vector<char>>
            v_coding_area(parity_num, std::vector<char>(block_size));
        for (int j = 0; j < parity_num; j++) {
          coding[j] = v_coding_area[j].data();
        }

        gettimeofday(&start_time, NULL);
        ec->perform_addition(data, coding, block_size,
            *partial_blocks_idx_ptr, *parity_idx_ptr);
        gettimeofday(&end_time, NULL);
        decoding_time += end_time.tv_sec - start_time.tv_sec +
            (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
        
        if (IF_DEBUG) {
          std::cout << "[Main Proxy " << self_cluster_id_
                    << "] Ready to decode with partial blocks!\n";
        }

        gettimeofday(&start_time, NULL);
        ec->decode_with_partial_blocks(coding, failures, block_size,
            *fls_idx_ptr, *parity_idx_ptr);
        gettimeofday(&end_time, NULL);
        decoding_time += end_time.tv_sec - start_time.tv_sec +
            (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      }
    } else {
      std::vector<char *> v_data(num_of_original_blocks);
      char **data = (char **)v_data.data();
      for (int j = 0; j < num_of_original_blocks; j++) {
        data[j] = (*original_blocks_ptr)[j].data();
      }
      
      auto partial_flags = std::vector<bool>(failed_num, true);

      gettimeofday(&start_time, NULL);
      ec->encode_partial_blocks(data, failures, block_size,
          *original_blocks_idx_ptr, *parity_idx_ptr,
          *fls_idx_ptr, *live_idx_ptr, partial_flags, true);
      gettimeofday(&end_time, NULL);
      decoding_time += end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    }

    std::vector<std::thread> writers;
    for (int i = 0; i < failed_num; i++) {
      int index = repair_plan.failed_blocks_index[i];
      unsigned int failed_block_id = repair_plan.failed_block_ids[i];
      writers.push_back(std::thread(send_to_datanode,
          failed_block_id, failures[i],
          repair_plan.new_locations[i].second.first,
          repair_plan.new_locations[i].second.second));
    }
    for (int i = 0; i < failed_num; i++) {
      writers[i].join();
    }

    if (IF_SIMULATE_CROSS_CLUSTER) {
      gettimeofday(&start_time, NULL);
      int cross_cluster_num = 0;
      for (int i = 0; i < (int)repair_plan.new_locations.size(); i++) {
        if(repair_plan.new_locations[i].first != self_cluster_id_) {
          cross_cluster_num++;   
        }
      }
      if (cross_cluster_num > 0) {
        int t_value_len = (int)block_size * cross_cluster_num;
        std::vector<char> t_value(t_value_len);
        transfer_to_networkcore(t_value.data(), t_value_len);
      }
      gettimeofday(&end_time, NULL);
      cross_cluster_time += end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    }

    if (IF_DEBUG) {
      std::cout << "[Main Proxy" << self_cluster_id_ << "] finish repair "
                << failed_num << " blocks! Decoding time : "
                << decoding_time << std::endl;
    }

    delete ec;

    MainRepairResp response;
    response.decoding_time = decoding_time;
    response.cross_cluster_time = cross_cluster_time;

    std::cout << ">?>?" << std::endl;
    return response;
  }

  void Proxy::help_repair(HelpRepairPlan repair_plan)
  {
    struct timeval start_time, end_time;
    double decoding_time = 0;

    bool if_partial_decoding = repair_plan.partial_decoding;
    bool partial_scheme = repair_plan.partial_scheme;
    bool t_flag = repair_plan.partial_less;
    int num_of_original_blocks = (int)repair_plan.inner_cluster_help_blocks_info.size();
    t_flag = (if_partial_decoding && t_flag);
    if (!t_flag && IF_DIRECT_FROM_NODE) {
      return;
    }

    auto ec = ec_factory(repair_plan.ec_type, repair_plan.cp);
    ec->init_coding_parameters(repair_plan.cp);
    size_t block_size = repair_plan.block_size;

    auto parity_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.parity_blocks_index);
    auto fls_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.failed_blocks_index);
    auto live_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.live_blocks_index);

    auto original_lock_ptr = std::make_shared<std::mutex>();
    auto original_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto original_blocks_idx_ptr = std::make_shared<std::vector<int>>();

    auto get_from_node = [this, original_lock_ptr, original_blocks_ptr,
                          original_blocks_idx_ptr, block_size](
                          unsigned int block_id, int block_idx,
                          std::string node_ip, int node_port) mutable
    {
      std::string block_id_str = std::to_string(block_id);
      std::vector<char> tmp_val(block_size);
      bool res = read_from_datanode(block_id_str.c_str(), block_id_str.size(), 
              tmp_val.data(), block_size, node_ip.c_str(), node_port);
      if (!res) {
        pthread_exit(NULL);
      }
      original_lock_ptr->lock();
      original_blocks_ptr->push_back(tmp_val);
      original_blocks_idx_ptr->push_back(block_idx);
      original_lock_ptr->unlock();
    };
    if (IF_DEBUG) {
      std::cout << "[Helper Proxy" << self_cluster_id_
                << "] Ready to read blocks from data node!" << std::endl;
    }

    if (num_of_original_blocks > 0) {
      std::vector<std::thread> readers;
      for (int i = 0; i < num_of_original_blocks; i++) {
        readers.push_back(std::thread(get_from_node, 
                          repair_plan.inner_cluster_help_block_ids[i], 
                          repair_plan.inner_cluster_help_blocks_info[i].first,
                          repair_plan.inner_cluster_help_blocks_info[i].second.first,
                          repair_plan.inner_cluster_help_blocks_info[i].second.second));
      }
      for (int i = 0; i < num_of_original_blocks; i++) {
        readers[i].join();
      }
    }

    my_assert(num_of_original_blocks == (int)original_blocks_ptr->size());

    int value_size = 0;

    if (t_flag) {
      if (IF_DEBUG) {
        std::cout << "[Helper Proxy" << self_cluster_id_
                  << "] partial encoding with blocks " << std::endl;
        for (auto it = original_blocks_idx_ptr->begin();
             it != original_blocks_idx_ptr->end(); it++) {
          std::cout << (*it) << " ";
        }
        std::cout << std::endl;
      }
      // encode partial blocks
      int partial_num = (int)parity_idx_ptr->size();
      if (partial_scheme) {
        partial_num = (int)fls_idx_ptr->size();
      }
      my_assert(partial_num > 0);
      std::vector<char *> v_data(num_of_original_blocks);
      std::vector<char *> v_coding(partial_num);
      char **data = (char **)v_data.data();
      char **coding = (char **)v_coding.data();
      std::vector<std::vector<char>> v_coding_area(partial_num, std::vector<char>(block_size));
      for (int j = 0; j < partial_num; j++) {
        coding[j] = v_coding_area[j].data();
      }
      for (int j = 0; j < num_of_original_blocks; j++) {
        data[j] = (*original_blocks_ptr)[j].data();
      }
      auto partial_flags = std::vector<bool>(partial_num, true);
      if (partial_scheme) {
        my_assert(parity_idx_ptr->size() > 0);
      }
      gettimeofday(&start_time, NULL);
      ec->encode_partial_blocks(data, coding, block_size,
          *original_blocks_idx_ptr, *parity_idx_ptr,
          *fls_idx_ptr, *live_idx_ptr, partial_flags, partial_scheme);
      gettimeofday(&end_time, NULL);
      decoding_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      int num_of_partial_blocks = 0;
      for (int j = 0; j < partial_num; j++) {
        if (partial_flags[j]) {
          num_of_partial_blocks++;
        }
      }
      my_assert(num_of_partial_blocks < num_of_original_blocks);

      // send to main proxy
      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::resolver resolver(io_context_);
      asio::error_code con_error;
      if (IF_DEBUG) {
        std::cout << "[Helper Proxy" << self_cluster_id_
                  << "] Try to connect main proxy port "
                  << repair_plan.main_proxy_port << std::endl;
      }
      asio::connect(socket_, resolver.resolve({repair_plan.main_proxy_ip,
          std::to_string(repair_plan.main_proxy_port)}), con_error);
      if (!con_error && IF_DEBUG) {
        std::cout << "[Helper Proxy" << self_cluster_id_
                  << "] Connect to " << repair_plan.main_proxy_ip
                  << ":" << repair_plan.main_proxy_port << " success!"
                  << std::endl;
      }
      std::vector<unsigned char>
          cid_buf = ECProject::int_to_bytes(self_cluster_id_);
      asio::write(socket_, asio::buffer(cid_buf, cid_buf.size()));
      std::vector<unsigned char> flag_buf = ECProject::int_to_bytes(1);
      asio::write(socket_, asio::buffer(flag_buf, flag_buf.size()));
      std::vector<unsigned char> num_buf = int_to_bytes(num_of_partial_blocks);
      asio::write(socket_, asio::buffer(num_buf, num_buf.size()));
      for (int j = 0; j < partial_num; j++) {
        if (partial_flags[j]) {
          std::vector<unsigned char> idx_buf;
          if (partial_scheme) {
            idx_buf = ECProject::int_to_bytes((*fls_idx_ptr)[j]);
          } else {
            idx_buf = ECProject::int_to_bytes((*parity_idx_ptr)[j]);
          }
          asio::write(socket_, asio::buffer(idx_buf, idx_buf.size()));
          asio::write(socket_, asio::buffer(coding[j], block_size));
          value_size += block_size;
        }
      }
      std::vector<unsigned char> decoding_time_buf = double_to_bytes(decoding_time);
      asio::write(socket_, asio::buffer(decoding_time_buf, decoding_time_buf.size()));
    } else if(!IF_DIRECT_FROM_NODE)  {
      // send to main proxy
      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::resolver resolver(io_context_);
      asio::error_code con_error;
      if (IF_DEBUG) {
        std::cout << "[Helper Proxy" << self_cluster_id_
                  << "] Try to connect main proxy port "
                  << repair_plan.main_proxy_port << std::endl;
      }
      asio::connect(socket_, resolver.resolve({repair_plan.main_proxy_ip,
            std::to_string(repair_plan.main_proxy_port)}), con_error);
      if (!con_error && IF_DEBUG) {
            std::cout << "[Helper Proxy" << self_cluster_id_
            << "] Connect to " << repair_plan.main_proxy_ip<< ":"
            << repair_plan.main_proxy_port << " success!" << std::endl;
      }
      
      std::vector<unsigned char>
          int_buf_self_cluster_id = ECProject::int_to_bytes(self_cluster_id_);
      asio::write(socket_, asio::buffer(int_buf_self_cluster_id,
          int_buf_self_cluster_id.size()));
      std::vector<unsigned char> flag_buf = ECProject::int_to_bytes(1);
      asio::write(socket_, asio::buffer(flag_buf, flag_buf.size()));
      std::vector<unsigned char>
          int_buf_num_of_blocks = int_to_bytes((int)original_blocks_idx_ptr->size());
      asio::write(socket_,
          asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()));

      int j = 0;
      for(auto it = original_blocks_idx_ptr->begin();
              it != original_blocks_idx_ptr->end(); it++, j++) { 
        // send index and value
        int block_idx = *it;
        std::vector<unsigned char> byte_block_idx = ECProject::int_to_bytes(block_idx);
        asio::write(socket_, asio::buffer(byte_block_idx, byte_block_idx.size()));
        asio::write(socket_, asio::buffer((*original_blocks_ptr)[j], block_size));
        value_size += block_size;
      }
    }
        
    if (IF_DEBUG) {
       std::cout << "[Helper Proxy" << self_cluster_id_
       << "] Send value to proxy" <<  repair_plan.main_proxy_port 
       << "! With length of " << ". Decoding time : " << decoding_time << std::endl;
    }
  }
}