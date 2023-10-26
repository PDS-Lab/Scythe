#include "tpcc_txn.h"
#include "coroutine_pool/coroutine_pool.h"
TxnStatus TxNewOrder(TPCC_SCHEMA* tpcc, Mode mode, PhasedLatency* phased_lat){
    /*
  "NEW_ORDER": {
  "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
  "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
  "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
  "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
  "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
  "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
  "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
  "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
  "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
  "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
  },
  */
    auto txn = TransactionFactory::TxnBegin(mode, (uint32_t)TPCCTableType::TableNum);
    FastRandom random_generator = *(tpcc->f_rand_[this_coroutine::current()->id()%8]);
    
    int warehouse_id_start_ = 1;
    int warehouse_id_end_ = tpcc->num_warehouse_;

    int district_id_start = 1;
    int district_id_end_ = tpcc->num_district_per_warehouse_;
    const uint32_t warehouse_id = tpcc->PickWarehouseId(random_generator, warehouse_id_start_, warehouse_id_end_);
    const uint32_t district_id = tpcc->RandomNumber(random_generator, district_id_start, district_id_end_);
    const uint32_t customer_id = tpcc->GetCustomerId(random_generator);
    int64_t c_key = tpcc->MakeCustomerKey(warehouse_id, district_id, customer_id);
    
    //涉及到的商品有99%是本仓库，另外1%远程调货
    
    int32_t all_local = 1;
    std::set<uint64_t> stock_set;  // remove identity stock ids;

    // local buffer used store stocks
    int64_t remote_stocks[tpcc_order_line_val_t::MAX_OL_CNT], local_stocks[tpcc_order_line_val_t::MAX_OL_CNT];
    int64_t remote_item_ids[tpcc_order_line_val_t::MAX_OL_CNT], local_item_ids[tpcc_order_line_val_t::MAX_OL_CNT];
    uint32_t local_supplies[tpcc_order_line_val_t::MAX_OL_CNT], remote_supplies[tpcc_order_line_val_t::MAX_OL_CNT];

    int num_remote_stocks(0), num_local_stocks(0);

    const int num_items = tpcc->RandomNumber(random_generator, tpcc_order_line_val_t::MIN_OL_CNT, tpcc_order_line_val_t::MAX_OL_CNT);

    for (int i = 0; i < num_items; i++) {
      int64_t item_id = tpcc->GetItemId(random_generator);
      if (tpcc->num_warehouse_ == 1 ||
          tpcc->RandomNumber(random_generator, 1, 100) > g_new_order_remote_item_pct) {
        // local stock case
        uint32_t supplier_warehouse_id = warehouse_id;
        int64_t s_key = tpcc->MakeStockKey(supplier_warehouse_id, item_id);
        if (stock_set.find(s_key) != stock_set.end()) {
          i--;
          continue;
        } else {
          stock_set.insert(s_key);
        }
        local_supplies[num_local_stocks] = supplier_warehouse_id;
        local_item_ids[num_local_stocks] = item_id;
        local_stocks[num_local_stocks++] = s_key;
      } else {
        // remote stock case
        int64_t s_key;
        uint32_t supplier_warehouse_id;
        do {
          supplier_warehouse_id =
              tpcc->RandomNumber(random_generator, 1, tpcc->num_warehouse_);
        } while (supplier_warehouse_id == warehouse_id);

        all_local = 0;

        s_key = tpcc->MakeStockKey(supplier_warehouse_id, item_id);
        if (stock_set.find(s_key) != stock_set.end()) {
          i--;
          continue;
        } else {
          stock_set.insert(s_key);
        }
        remote_stocks[num_remote_stocks] = s_key;
        remote_supplies[num_remote_stocks] = supplier_warehouse_id;
        remote_item_ids[num_remote_stocks++] = item_id;
      }
    }

    //run dtxn
    if(phased_lat){
        gettimeofday(&(phased_lat->exe_start_tv),nullptr);
    }
    
    //step1: get*3
    tpcc_warehouse_key_t ware_key;
    ware_key.w_id = warehouse_id;
    auto ware_obj = txn->GetObject(ware_key.item_key,(uint32_t)TPCCTableType::kWarehouseTable, sizeof(tpcc_warehouse_val_t));
    std::vector<TxnObjPtr> read_set;
    // read_set.push_back(ware_obj);
    auto rc = txn->Read(ware_obj);
    if(rc!=TxnStatus::OK){
      txn->Rollback();
      return rc;
    }
    //getCustomer
    tpcc_customer_key_t cust_key;
    cust_key.c_id = c_key;
    auto cust_obj = txn->GetObject(cust_key.item_key,(uint32_t)TPCCTableType::kCustomerTable,sizeof(tpcc_customer_val_t));
    //read_set.push_back(cust_obj);
    rc = txn->Read(cust_obj);
    if(rc!=TxnStatus::OK){
      txn->Rollback();
      return rc;
    }
    //getDistrict
    uint64_t dkey = tpcc->MakeDistrictKey(warehouse_id,district_id);
    tpcc_district_key_t dist_key;
    dist_key.d_id = dkey;
    auto dist_obj = txn->GetObject(dist_key.item_key,(uint32_t)TPCCTableType::kDistrictTable,sizeof(tpcc_district_val_t));
    rc = txn->Read(dist_obj);
    if(rc!=TxnStatus::OK){
      txn->Rollback();
      return rc;
    }
    // read_set.push_back(dist_obj);
    // auto rc = txn->Read(read_set);
    // if(rc!=TxnStatus::OK){
    //   // if(mode==Mode::HOT)
    //   //   LOG_ERROR("rc:%d",rc);
    //   txn->Rollback();
    //   return rc;
    // }
    
    //incrementNextOrderId
    auto dist_val = dist_obj->get_as<tpcc_district_val_t>();
    const auto my_next_o_id = dist_val->d_next_o_id;
    dist_val->d_next_o_id+=1;
    txn->Write(dist_obj);
    //createNewOrder
    uint64_t no_key = tpcc->MakeNewOrderKey(warehouse_id, district_id, my_next_o_id);
    tpcc_new_order_key_t norder_key;
    norder_key.no_id = no_key;
    auto norder_obj = txn->GetObject(norder_key.item_key,(uint32_t)TPCCTableType::kNewOrderTable,sizeof(tpcc_new_order_val_t));
    norder_obj->set_new();
    auto norder_val = norder_obj->get_as<tpcc_new_order_val_t>();
    norder_val->debug_magic = tpcc_add_magic;
    txn->Write(norder_obj);

    //createOrder
    uint64_t o_key = tpcc->MakeOrderKey(warehouse_id, district_id, my_next_o_id);
    tpcc_order_key_t order_key;
    order_key.o_id = o_key;
    auto order_obj = txn->GetObject(order_key.item_key,(uint32_t)TPCCTableType::kOrderTable,sizeof(tpcc_order_val_t));
    order_obj->set_new();
    auto order_val = order_obj->get_as<tpcc_order_val_t>();
    order_val->o_c_id = int32_t(customer_id);
    order_val->o_carrier_id = 0;
    order_val->o_ol_cnt = num_items;
    order_val->o_all_local = all_local;
    order_val->o_entry_d = tpcc->GetCurrentTimeMillis();
    txn->Write(order_obj);

    //whether to do: index table 
    uint64_t o_index_key = tpcc->MakeOrderIndexKey(warehouse_id, district_id, customer_id, my_next_o_id);
    tpcc_order_index_key_t order_index_key;
    order_index_key.o_index_id = o_index_key;
    auto oidx_obj = txn->GetObject(order_index_key.item_key,(uint32_t)TPCCTableType::kOrderIndexTable,sizeof(tpcc_order_index_val_t));
    oidx_obj->set_new();
    tpcc_order_index_val_t* oidx_val = (tpcc_order_index_val_t*)oidx_obj->get_as<tpcc_order_index_val_t>();
    oidx_val->o_id = o_key;
    oidx_val->debug_magic = tpcc_add_magic;
    txn->Write(oidx_obj);
    //getItemInfo
    //getStockInfo
    //updateStock
    //createOrderLine
    for (int ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
      //#define ol_number 1
      const int64_t ol_i_id = local_item_ids[ol_number - 1];
      const uint32_t ol_quantity = tpcc->RandomNumber(random_generator, 1, 10);
      // read item info
      tpcc_item_key_t tpcc_item_key;
      tpcc_item_key.i_id = ol_i_id;

      auto item_obj = txn->GetObject(tpcc_item_key.item_key,(uint32_t)TPCCTableType::kItemTable,sizeof(tpcc_item_val_t));
      rc = txn->Read(item_obj);
      if(rc!=TxnStatus::OK){
        txn->Rollback();
        return rc;
      }
      //else LOG_DEBUG("read item succeeded");

      // read and update stock info
      int64_t s_key = local_stocks[ol_number - 1];
      tpcc_stock_key_t stock_key;
      stock_key.s_id = s_key;
      auto stock_obj = txn->GetObject(stock_key.item_key,(uint32_t)TPCCTableType::kStockTable,sizeof(tpcc_stock_val_t));
      rc = txn->Read(stock_obj);
      if(rc!=TxnStatus::OK){
        txn->Rollback();
        return rc;
      }

      tpcc_item_val_t* item_val = item_obj->get_as<tpcc_item_val_t>();
      tpcc_stock_val_t* stock_val = stock_obj->get_as<tpcc_stock_val_t>();
      
      //else LOG_DEBUG("read stock succeeded");

      if (stock_val->s_quantity - ol_quantity >= 10) {
        stock_val->s_quantity -= ol_quantity;
      } else {
        stock_val->s_quantity += (-int32_t(ol_quantity) + 91);
      }

      stock_val->s_ytd += ol_quantity;
      stock_val->s_remote_cnt += (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;

      // insert order line record
      int64_t ol_key = tpcc->MakeOrderLineKey(warehouse_id, district_id, my_next_o_id, ol_number);
      tpcc_order_line_key_t order_line_key;
      order_line_key.ol_id = ol_key;
      auto ol_obj = txn->GetObject(order_line_key.item_key,(uint32_t)TPCCTableType::kOrderLineTable,sizeof(tpcc_order_line_val_t));
      ol_obj->set_new();
      tpcc_order_line_val_t* order_line_val = ol_obj->get_as<tpcc_order_line_val_t>();

      order_line_val->ol_i_id = int32_t(ol_i_id);
      order_line_val->ol_delivery_d = 0;  // not delivered yet
      order_line_val->ol_amount = float(ol_quantity) * item_val->i_price;
      order_line_val->ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
      order_line_val->ol_quantity = int8_t(ol_quantity);
      order_line_val->debug_magic = tpcc_add_magic;
      txn->Write(ol_obj);
    }

    for (int ol_number = 1; ol_number <= num_remote_stocks; ol_number++) {
      const int64_t ol_i_id = remote_item_ids[ol_number - 1];
      const uint32_t ol_quantity = tpcc->RandomNumber(random_generator, 1, 10);
      // read item info
      tpcc_item_key_t tpcc_item_key;
      tpcc_item_key.i_id = ol_i_id;

      auto item_obj = txn->GetObject(tpcc_item_key.item_key,(uint32_t)TPCCTableType::kItemTable,sizeof(tpcc_item_val_t));
      rc = txn->Read(item_obj);
      if(rc!=TxnStatus::OK){
        txn->Rollback();
        return rc;
      }
      int64_t s_key = remote_stocks[ol_number - 1];
      // read and update stock info
      tpcc_stock_key_t stock_key;
      stock_key.s_id = s_key;

      auto stock_obj = txn->GetObject(stock_key.item_key,(uint32_t)TPCCTableType::kStockTable,sizeof(tpcc_stock_val_t));
      rc = txn->Read(stock_obj);
      if(rc!=TxnStatus::OK){
        txn->Rollback();
        return rc;
      }

      tpcc_item_val_t* item_val = (tpcc_item_val_t*)item_obj->get_as<tpcc_item_val_t>();
      tpcc_stock_val_t* stock_val = (tpcc_stock_val_t*)stock_obj->get_as<tpcc_stock_val_t>();


      if (stock_val->s_quantity - ol_quantity >= 10) {
        stock_val->s_quantity -= ol_quantity;
      } else {
        stock_val->s_quantity += (-int32_t(ol_quantity) + 91);
      }

      stock_val->s_ytd += ol_quantity;
      stock_val->s_remote_cnt += (remote_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;

      // insert order line record
      int64_t ol_key = tpcc->MakeOrderLineKey(warehouse_id, district_id, my_next_o_id, num_local_stocks + ol_number);
      tpcc_order_line_key_t order_line_key;
      order_line_key.ol_id = ol_key;
      // RDMA_LOG(DBG) << warehouse_id << " " << district_id << " " << my_next_o_id << " " <<  num_local_stocks + ol_number << ". ol_key: " << ol_key;
      auto ol_obj = txn->GetObject(order_line_key.item_key,(uint32_t)TPCCTableType::kOrderLineTable,sizeof(tpcc_order_line_val_t));
      ol_obj->set_new();
      tpcc_order_line_val_t* order_line_val = ol_obj->get_as<tpcc_order_line_val_t>();
      order_line_val->ol_i_id = int32_t(ol_i_id);
      order_line_val->ol_delivery_d = 0;  // not delivered yet
      order_line_val->ol_amount = float(ol_quantity) * item_val->i_price;
      order_line_val->ol_supply_w_id = int32_t(remote_supplies[ol_number - 1]);
      order_line_val->ol_quantity = int8_t(ol_quantity);
      order_line_val->debug_magic = tpcc_add_magic;
      txn->Write(ol_obj);
    }

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
    }
    return txn->Commit();
}
TxnStatus TxPayment(TPCC_SCHEMA* tpcc, Mode mode, PhasedLatency* phased_lat){
    auto txn = TransactionFactory::TxnBegin(mode, (uint32_t)TPCCTableType::TableNum);
    FastRandom random_generator = *(tpcc->f_rand_[this_coroutine::current()->id()%8]);
    int x = tpcc->RandomNumber(random_generator, 1, 100);
    int y = tpcc->RandomNumber(random_generator, 1, 100);
    
    int warehouse_id_start_ = 1;
    int warehouse_id_end_ = tpcc->num_warehouse_;

    int district_id_start = 1;
    int district_id_end_ = tpcc->num_district_per_warehouse_;

    const uint32_t warehouse_id = tpcc->PickWarehouseId(random_generator, warehouse_id_start_, warehouse_id_end_);
    const uint32_t district_id = tpcc->RandomNumber(random_generator, district_id_start, district_id_end_);

    int32_t c_w_id = warehouse_id;
    int32_t c_d_id = district_id;
  uint32_t customer_id = 0;
  // The payment amount (H_AMOUNT) is randomly selected within [1.00 .. 5,000.00].
  float h_amount = (float)tpcc->RandomNumber(random_generator, 100, 500000) / 100.0;
  if (y <= 60) {
    // 60%: payment by last name
    char last_name[tpcc_customer_val_t::MAX_LAST + 1];
    size_t size = (tpcc->GetNonUniformCustomerLastNameLoad(random_generator)).size();
    LOG_ASSERT(tpcc_customer_val_t::MAX_LAST - size >= 0,"tpcc_customer_val_t::MAX_LAST - size < 0");
    strcpy(last_name, tpcc->GetNonUniformCustomerLastNameLoad(random_generator).c_str());
    // FIXME:: Find customer by the last name
    // All rows in the CUSTOMER table with matching C_W_ID, C_D_ID and C_LAST are selected sorted by C_FIRST in ascending order.
    // Let n be the number of rows selected.
    // C_ID, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
    // and C_BALANCE are retrieved from the row at position (n/ 2 rounded up to the next integer) in the sorted set of selected rows from the CUSTOMER table.
    customer_id = tpcc->GetCustomerId(random_generator);
  } else {
    // 40%: payment by id
    LOG_ASSERT(y > 60,"y <= 60");
    customer_id = tpcc->GetCustomerId(random_generator);
  }
  //Run
  if(phased_lat){
        gettimeofday(&(phased_lat->exe_start_tv),nullptr);
  }
  tpcc_warehouse_key_t ware_key;
  ware_key.w_id = warehouse_id;
  auto ware_obj = txn->GetObject(ware_key.item_key,(uint32_t)TPCCTableType::kWarehouseTable,sizeof(tpcc_warehouse_val_t));
  auto rc = txn->Read(ware_obj);
  if(rc!=TxnStatus::OK){
    return rc;
  }
  //dtx->AddToReadWriteSet(ware_obj);

  uint64_t d_key = tpcc->MakeDistrictKey(warehouse_id, district_id);
  tpcc_district_key_t dist_key;
  dist_key.d_id = d_key;
  auto dist_obj = txn->GetObject(dist_key.item_key,(uint32_t)TPCCTableType::kDistrictTable,sizeof(tpcc_district_val_t));
  rc = txn->Read(dist_obj);
  if(rc!=TxnStatus::OK){
    txn->Rollback();
    return rc;
  }

  tpcc_customer_key_t cust_key;
  cust_key.c_id = tpcc->MakeCustomerKey(c_w_id, c_d_id, customer_id);
  auto cust_obj = txn->GetObject(cust_key.item_key,(uint32_t)TPCCTableType::kCustomerTable,sizeof(tpcc_customer_val_t));
  rc = txn->Read(cust_obj);
  if(rc!=TxnStatus::OK){
    txn->Rollback();
    return rc;
  }

  tpcc_history_key_t hist_key;
  hist_key.h_id = tpcc->MakeHistoryKey(warehouse_id, district_id, c_w_id, c_d_id, customer_id);
  auto hist_obj = txn->GetObject(hist_key.item_key,(uint32_t)TPCCTableType::kHistoryTable,sizeof(tpcc_history_val_t));
  rc = txn->Read(hist_obj);
  if(rc!=TxnStatus::OK){
    txn->Rollback();
    return rc;
  }
  tpcc_warehouse_val_t* ware_val = ware_obj->get_as<tpcc_warehouse_val_t>();
  std::string check(ware_val->w_zip);

  tpcc_district_val_t* dist_val = dist_obj->get_as<tpcc_district_val_t>();
  check = std::string(dist_val->d_zip);

  tpcc_customer_val_t* cust_val = cust_obj->get_as<tpcc_customer_val_t>();
  
  ware_val->w_ytd += h_amount;
  dist_val->d_ytd += h_amount;

  cust_val->c_balance -= h_amount;
  cust_val->c_ytd_payment += h_amount;
  cust_val->c_payment_cnt += 1;

  if (strcmp(cust_val->c_credit, BAD_CREDIT) == 0) {
    // Bad credit: insert history into c_data
    static const int HISTORY_SIZE = tpcc_customer_val_t::MAX_DATA + 1;
    char history[HISTORY_SIZE];
    int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
                              customer_id, c_d_id, c_w_id, district_id, warehouse_id, h_amount);
    assert(characters < HISTORY_SIZE);

    // Perform the insert with a move and copy
    int current_keep = static_cast<int>(strlen(cust_val->c_data));
    if (current_keep + characters > tpcc_customer_val_t::MAX_DATA) {
      current_keep = tpcc_customer_val_t::MAX_DATA - characters;
    }
    assert(current_keep + characters <= tpcc_customer_val_t::MAX_DATA);
    memmove(cust_val->c_data + characters, cust_val->c_data, current_keep);
    memcpy(cust_val->c_data, history, characters);
    cust_val->c_data[characters + current_keep] = '\0';
    assert(strlen(cust_val->c_data) == characters + current_keep);
  }
  tpcc_history_val_t* hist_val = hist_obj->get_as<tpcc_history_val_t>();

  hist_val->h_date = tpcc->GetCurrentTimeMillis();  // different time at server and client cause errors?
  hist_val->h_amount = h_amount;
  strcpy(hist_val->h_data, ware_val->w_name);
  strcat(hist_val->h_data, "    ");
  strcat(hist_val->h_data, dist_val->d_name);
  
  txn->Write(ware_obj);
  txn->Write(dist_obj);
  txn->Write(cust_obj);
  txn->Write(hist_obj);
  if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
  }
  return txn->Commit();
}

TxnStatus TxDelivery(TPCC_SCHEMA* tpcc, Mode mode, PhasedLatency* phased_lat){
  auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)TPCCTableType::TableNum);
  FastRandom random_generator = *(tpcc->f_rand_[this_coroutine::current()->id()%8]);
  int warehouse_id_start_ = 1;
  int warehouse_id_end_ = tpcc->num_warehouse_;
  const uint32_t warehouse_id = tpcc->PickWarehouseId(random_generator, warehouse_id_start_, warehouse_id_end_);
  const int o_carrier_id = tpcc->RandomNumber(random_generator, tpcc_order_val_t::MIN_CARRIER_ID, tpcc_order_val_t::MAX_CARRIER_ID);
  const uint32_t current_ts = tpcc->GetCurrentTimeMillis();
  if(phased_lat){
      gettimeofday(&(phased_lat->exe_start_tv),nullptr);
  }
  for (int d_id = 1; d_id <= tpcc->num_district_per_warehouse_; d_id++) {
    // FIXME: select the lowest NO_O_ID with matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID) in the NEW-ORDER table
    int min_o_id = tpcc->num_customer_per_district_ * tpcc_new_order_val_t::SCALE_CONSTANT_BETWEEN_NEWORDER_ORDER + 1;
    int max_o_id = tpcc->num_customer_per_district_;
    int o_id = tpcc->RandomNumber(random_generator, min_o_id, max_o_id);

    int64_t no_key = tpcc->MakeNewOrderKey(warehouse_id, d_id, o_id);
    tpcc_new_order_key_t norder_key;
    norder_key.no_id = no_key;
    auto norder_obj = txn->GetObject(norder_key.item_key,(uint32_t)TPCCTableType::kNewOrderTable,sizeof(tpcc_new_order_val_t));
    auto rc = txn->Read(norder_obj);
    if(rc!=TxnStatus::OK){
      //continue;
      txn->Rollback();
      return rc;
    }
    
    // Add the new order obj to read write set to be deleted
    //dtx->AddToReadWriteSet(norder_obj);

    uint64_t o_key = tpcc->MakeOrderKey(warehouse_id, d_id, o_id);
    tpcc_order_key_t order_key;
    order_key.o_id = o_key;
    auto order_obj = txn->GetObject(order_key.item_key,(uint32_t)TPCCTableType::kOrderTable,sizeof(tpcc_order_val_t));
    rc = txn->Read(order_obj);
    if(rc!=TxnStatus::OK){
      txn->Rollback();
      return rc;
    }
    //dtx->AddToReadWriteSet(order_obj);

    // The row in the ORDER table with matching O_W_ID (equals W_ ID), O_D_ID (equals D_ID), and O_ID (equals NO_O_ID) is selected

    // o_entry_d never be 0
    tpcc_order_val_t* order_val = order_obj->get_as<tpcc_order_val_t>();
    // if (order_val->o_entry_d == 0) {
    //   RDMA_LOG(FATAL) << "[FATAL] Read order unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    // }

    // O_C_ID, the customer number, is retrieved
    int32_t customer_id = order_val->o_c_id;

    // O_CARRIER_ID is updated
    order_val->o_carrier_id = o_carrier_id;

    // All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected.
    // All OL_DELIVERY_D, the delivery dates, are updated to the current system time
    // The sum of all OL_AMOUNT is retrieved
    float sum_ol_amount = 0;
    for (int line_number = 1; line_number <= tpcc_order_line_val_t::MAX_OL_CNT; ++line_number) {
      int64_t ol_key = tpcc->MakeOrderLineKey(warehouse_id, d_id, o_id, line_number);
      tpcc_order_line_key_t order_line_key;
      order_line_key.ol_id = ol_key;
      auto ol_obj = txn->GetObject(order_line_key.item_key,(uint32_t)TPCCTableType::kOrderLineTable,sizeof(tpcc_order_line_val_t));
      rc = txn->Read(ol_obj);
      if (rc!=TxnStatus::OK) {
        //continue;
        txn->Rollback();
        return rc;
      }
      tpcc_order_line_val_t* order_line_val = ol_obj->get_as<tpcc_order_line_val_t>();
      //check
      {
        if (order_line_val->debug_magic != tpcc_add_magic) {
          LOG_FATAL("[FATAL] Read order line unmatch, check:%lu, magic:%lu",order_line_val->debug_magic,tpcc_add_magic);
        }
      }
      order_line_val->ol_delivery_d = current_ts;
      sum_ol_amount += order_line_val->ol_amount;
      txn->Write(ol_obj);
    }

    // The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID (equals D_ID), and C_ID (equals O_C_ID) is selected
    tpcc_customer_key_t cust_key;
    cust_key.c_id = tpcc->MakeCustomerKey(warehouse_id, d_id, customer_id);
    auto cust_obj = txn->GetObject(cust_key.item_key,(uint32_t)TPCCTableType::kCustomerTable,sizeof(tpcc_customer_val_t));
    rc = txn->Read(cust_obj);
    if(rc!=TxnStatus::OK){
      txn->Rollback();
      return rc;
    }
    tpcc_customer_val_t* cust_val = cust_obj->get_as<tpcc_customer_val_t>();
    
    // C_BALANCE is increased by the sum of all order-line amounts (OL_AMOUNT) previously retrieved
    cust_val->c_balance += sum_ol_amount;

    // C_DELIVERY_CNT is incremented by 1
    cust_val->c_delivery_cnt += 1;
    txn->Write(cust_obj);
  }
  if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
  }
  return txn->Commit();
}

//read only txn
TxnStatus TxOrderStatus(TPCC_SCHEMA* tpcc, Mode mode, PhasedLatency* phased_lat){
  auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)TPCCTableType::TableNum);
  FastRandom random_generator = *(tpcc->f_rand_[this_coroutine::current()->id()%8]);
  int y = tpcc->RandomNumber(random_generator, 1, 100);

  int warehouse_id_start_ = 1;
  int warehouse_id_end_ = tpcc->num_warehouse_;

  int district_id_start = 1;
  int district_id_end_ = tpcc->num_district_per_warehouse_;

  const uint32_t warehouse_id = tpcc->PickWarehouseId(random_generator, warehouse_id_start_, warehouse_id_end_);
  const uint32_t district_id = tpcc->RandomNumber(random_generator, district_id_start, district_id_end_);
  uint32_t customer_id = 0;

  if (y <= 60) {
    // FIXME:: Find customer by the last name
    customer_id = tpcc->GetCustomerId(random_generator);
  } else {
    customer_id = tpcc->GetCustomerId(random_generator);
  }

  //run
  if(phased_lat){
    gettimeofday(&(phased_lat->exe_start_tv),nullptr);
  }
  std::vector<TxnObjPtr> read_set;
  tpcc_customer_key_t cust_key;
  cust_key.c_id = tpcc->MakeCustomerKey(warehouse_id, district_id, customer_id);
  auto cust_obj = txn->GetObject(cust_key.item_key,(uint32_t)TPCCTableType::kCustomerTable,sizeof(tpcc_customer_val_t));
  read_set.push_back(cust_obj);

  int32_t order_id = tpcc->RandomNumber(random_generator, 1, tpcc->num_customer_per_district_);
  uint64_t o_key = tpcc->MakeOrderKey(warehouse_id, district_id, order_id);
  tpcc_order_key_t order_key;
  order_key.o_id = o_key;
  auto order_obj = txn->GetObject(order_key.item_key,(uint32_t)TPCCTableType::kOrderTable,sizeof(tpcc_order_val_t));
  read_set.push_back(order_obj);

  auto rc = txn->Read(read_set);
  read_set.clear();
  
  //dtx->AddToReadOnlySet(order_obj);
  tpcc_customer_val_t* cust_val = cust_obj->get_as<tpcc_customer_val_t>();

  // o_entry_d never be 0
  tpcc_order_val_t* order_val = (tpcc_order_val_t*)order_obj->get_as<tpcc_order_val_t>();

  for (int i = 1; i <= order_val->o_ol_cnt; i++) {
    int64_t ol_key = tpcc->MakeOrderLineKey(warehouse_id, district_id, order_id, i);
    tpcc_order_line_key_t order_line_key;
    order_line_key.ol_id = ol_key;
    auto ol_obj = txn->GetObject(order_line_key.item_key,(uint32_t)TPCCTableType::kOrderLineTable,sizeof(tpcc_order_line_val_t));
    read_set.push_back(ol_obj);
  }
  rc = txn->Read(read_set);
  if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
  }
  return txn->Commit();
}

TxnStatus TxStockLevel(TPCC_SCHEMA* tpcc, Mode mode, PhasedLatency* phased_lat){
  auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)TPCCTableType::TableNum);
  FastRandom random_generator = *(tpcc->f_rand_[this_coroutine::current()->id()%8]);
  int32_t threshold = tpcc->RandomNumber(random_generator, tpcc_stock_val_t::MIN_STOCK_LEVEL_THRESHOLD, tpcc_stock_val_t::MAX_STOCK_LEVEL_THRESHOLD);

  int warehouse_id_start_ = 1;
  int warehouse_id_end_ = tpcc->num_warehouse_;

  int district_id_start = 1;
  int district_id_end_ = tpcc->num_district_per_warehouse_;

  const uint32_t warehouse_id = tpcc->PickWarehouseId(random_generator, warehouse_id_start_, warehouse_id_end_);
  const uint32_t district_id = tpcc->RandomNumber(random_generator, district_id_start, district_id_end_);
  
  uint64_t d_key = tpcc->MakeDistrictKey(warehouse_id, district_id);
  tpcc_district_key_t dist_key;
  dist_key.d_id = d_key;

  if(phased_lat){
      gettimeofday(&(phased_lat->exe_start_tv),nullptr);
  }

  auto dist_obj = txn->GetObject(dist_key.item_key,(uint32_t)TPCCTableType::kDistrictTable,sizeof(tpcc_district_val_t));
  auto rc = txn->Read(dist_obj);

  tpcc_district_val_t* dist_val = dist_obj->get_as<tpcc_district_val_t>();
  std::string check = std::string(dist_val->d_zip);
  
  int32_t o_id = dist_val->d_next_o_id;

  std::vector<int32_t> s_i_ids;
  s_i_ids.reserve(300);

  for (int order_id = o_id - tpcc_stock_val_t::STOCK_LEVEL_ORDERS; order_id < o_id; ++order_id) {
    // Populate line_numer is random: [Min_OL_CNT, MAX_OL_CNT)
    for (int line_number = 1; line_number <= tpcc_order_line_val_t::MAX_OL_CNT; ++line_number) {
      int64_t ol_key = tpcc->MakeOrderLineKey(warehouse_id, district_id, order_id, line_number);
      tpcc_order_line_key_t order_line_key;
      order_line_key.ol_id = ol_key;
      auto ol_obj = txn->GetObject(order_line_key.item_key,(uint32_t)TPCCTableType::kOrderLineTable,sizeof(tpcc_order_line_val_t));
      //TODO
      if (txn->Read(ol_obj)!=TxnStatus::OK) {
        // Not found, not abort
        // 这里应该是正常就会有一次读不到的情况作为结束，说明对应的order根本没生成那么多order line
        break;
      }
      tpcc_order_line_val_t* ol_val = ol_obj->get_as<tpcc_order_line_val_t>();
      int64_t s_key = tpcc->MakeStockKey(warehouse_id, ol_val->ol_i_id);
      tpcc_stock_key_t stock_key;
      stock_key.s_id = s_key;
      auto stock_obj = txn->GetObject(stock_key.item_key,(uint32_t)TPCCTableType::kStockTable,sizeof(tpcc_stock_val_t));
      rc = txn->Read(stock_obj);
      tpcc_stock_val_t* stock_val = stock_obj->get_as<tpcc_stock_val_t>();

      if (stock_val->s_quantity < threshold) {
        s_i_ids.push_back(ol_val->ol_i_id);
      }
    }
  }
  std::sort(s_i_ids.begin(), s_i_ids.end());
  int num_distinct = 0;  // The output of this transaction
  int32_t last = -1;     // -1 is an invalid s_i_id
  for (size_t i = 0; i < s_i_ids.size(); ++i) {
    if (s_i_ids[i] != last) {
      last = s_i_ids[i];
      num_distinct += 1;
    }
  }
  if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
  }
  return txn->Commit();
}


TxnStatus TxTestReadWrite(){
  TxnStatus rc = TxnStatus::OK;
  do{
    auto txn = TransactionFactory::TxnBegin(Mode::COLD,(uint32_t)TPCCTableType::TableNum);
    int32_t warehouse_id = 1;
    tpcc_warehouse_key_t ware_key;
    ware_key.w_id = warehouse_id;
    auto ware_obj = txn->GetObject(ware_key.item_key,static_cast<uint32_t>(TPCCTableType::kWarehouseTable),sizeof(tpcc_warehouse_val_t));
    txn->Read(ware_obj);
    auto ware_val = ware_obj->get_as<tpcc_warehouse_val_t>();
    std::string check(ware_val->w_zip);
    LOG_INFO("zip: %s",check.c_str());
    LOG_ASSERT(check==tpcc_zip_magic,"unexpected zip, %s", check.c_str());
    //strncpy(ware_val->w_zip,"987654321\0",10);
    txn->Write(ware_obj);
    rc = txn->Commit();
    if(rc!=TxnStatus::OK){
      LOG_DEBUG("txn retry");
    }
  }while(rc!=TxnStatus::OK);
  auto new_txn = TransactionFactory::TxnBegin();
  int32_t warehouse_id = 1;
  tpcc_warehouse_key_t ware_key;
  ware_key.w_id = warehouse_id;
  auto ware_obj = new_txn->GetObject(ware_key.item_key,static_cast<uint32_t>(TPCCTableType::kWarehouseTable),sizeof(tpcc_warehouse_val_t));
  new_txn->Read(ware_obj);
  auto ware_val = ware_obj->get_as<tpcc_warehouse_val_t>();
  LOG_DEBUG("now w_zip:%s",ware_val->w_zip);
  rc = new_txn->Commit();
  return rc;
}