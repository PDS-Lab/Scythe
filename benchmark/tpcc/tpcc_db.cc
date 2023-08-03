#include "tpcc/tpcc_db.h"
#include "dtxn/dtxn.h"

using benchmark::FastRandom;
TPCCTxType* TPCC_SCHEMA::CreateWorkgenArray() {
    TPCCTxType* workgen_arr = new TPCCTxType[100];

    int i = 0, j = 0;

    j += FREQUENCY_NEW_ORDER;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kNewOrder;

    j += FREQUENCY_PAYMENT;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kPayment;

    j += FREQUENCY_ORDER_STATUS;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kOrderStatus;

    j += FREQUENCY_DELIVERY;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kDelivery;

    j += FREQUENCY_STOCK_LEVEL;
    for (; i < j; i++) workgen_arr[i] = TPCCTxType::kStockLevel;

    assert(i == 100 && j == 100);
    return workgen_arr;
}

void TPCC_SCHEMA::LoadTable(){
    warehouse_table_ = new KVEngine();
    dbs[(size_t)TPCCTableType::kWarehouseTable] = warehouse_table_;
    PopulateWarehouseTable(9324);

    // district_table_ = new KVEngine();
    // dbs[(size_t)TPCCTableType::kDistrictTable] = district_table_;
    // PopulateDistrictTable(129856349);

    // customer_table_ = new KVEngine();
    // customer_index_table_ = new KVEngine();
    // history_table_ = new KVEngine();
    // dbs[(size_t)TPCCTableType::kCustomerTable] = customer_table_;
    // dbs[(size_t)TPCCTableType::kCustomerIndexTable] = customer_index_table_;
    // dbs[(size_t)TPCCTableType::kHistoryTable] = history_table_;
    // PopulateCustomerAndHistoryTable(923587856425);

    // order_table_ = new KVEngine();
    // order_line_table_ = new KVEngine();
    // order_index_table_ = new KVEngine();
    // new_order_table_ = new KVEngine();
    // dbs[(size_t)TPCCTableType::kOrderTable] = order_table_;
    // dbs[(size_t)TPCCTableType::kOrderLineTable] = order_line_table_;
    // dbs[(size_t)TPCCTableType::kOrderIndexTable] = order_index_table_;
    // dbs[(size_t)TPCCTableType::kNewOrderTable] = new_order_table_;
    // PopulateOrderNewOrderAndOrderLineTable(2343352);

    // stock_table_ = new KVEngine();
    // dbs[(size_t)TPCCTableType::kStockTable] = stock_table_;
    // PopulateStockTable(89785943);

    // item_table_ = new KVEngine();
    // dbs[(size_t)TPCCTableType::kItemTable] = item_table_;
    // PopulateItemTable(235443);
}

void TPCC_SCHEMA::PopulateWarehouseTable(uint64_t seed){
    LOG_DEBUG("PopulateWarehouseTable");
    int total_warehouse_records_inserted = 0, total_warehouse_records_examined = 0;
    benchmark::FastRandom random_generator(seed);
    int cnt = 0;
    for(uint32_t w_id = 1;w_id <= num_warehouse_;w_id++){
        tpcc_warehouse_key_t warehouse_key;
        warehouse_key.w_id = w_id;

        /* Initialize the warehouse payload */
        tpcc_warehouse_val_t warehouse_val;
        warehouse_val.w_ytd = 300000 * 100;
        warehouse_val.w_tax = (float)RandomNumber(random_generator, 0, 2000) / 10000.0;
        strcpy(warehouse_val.w_name,
            RandomStr(random_generator, RandomNumber(random_generator, tpcc_warehouse_val_t::MIN_NAME, tpcc_warehouse_val_t::MAX_NAME)).c_str());
        strcpy(warehouse_val.w_street_1,
            RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
        strcpy(warehouse_val.w_street_2,
            RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
        strcpy(warehouse_val.w_city,
            RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
        strcpy(warehouse_val.w_state, RandomStr(random_generator, Address::STATE).c_str());
        strcpy(warehouse_val.w_zip, "123456789");

        assert(warehouse_val.w_state[2] == '\0' && strcmp(warehouse_val.w_zip, "123456789") == 0);
        
        auto ts = TSO::get_ts();
        warehouse_table_->put(warehouse_key.item_key, &warehouse_val,sizeof(warehouse_val), ts);
        
        // ReadResult res;
        // res.buf_size = sizeof(warehouse_val);
        // res.buf = (void *)malloc(res.buf_size * 2);
        // warehouse_table_->get(warehouse_key.item_key,res,ts,false);
        // auto val = static_cast<tpcc_warehouse_val_t*>(res.buf);
        // LOG_DEBUG("magic num:%s, w_name:%s",val->w_zip,val->w_name);
        // ++cnt;
    }
    //LOG_INFO("%d",cnt);
}

void TPCC_SCHEMA::PopulateDistrictTable(uint64_t seed){
    int total_district_records_inserted = 0, total_district_records_examined = 0;
  FastRandom random_generator(seed);
  for (uint32_t w_id = 1; w_id <= num_warehouse_; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse_; d_id++) {
      tpcc_district_key_t district_key;
      district_key.d_id = MakeDistrictKey(w_id, d_id);

      /* Initialize the district payload */
      tpcc_district_val_t district_val;

      district_val.d_ytd = 30000 * 100;  // different from warehouse, notice it did the scale up
      //  NOTICE:: scale should check consistency requirements.
      //  D_YTD = sum(H_AMOUNT) where (D_W_ID, D_ID) = (H_W_ID, H_D_ID).
      district_val.d_tax = (float)RandomNumber(random_generator, 0, 2000) / 10000.0;
      district_val.d_next_o_id = num_customer_per_district_ + 1;
      //  NOTICE:: scale should check consistency requirements.
      //  D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)

      strcpy(district_val.d_name,
             RandomStr(random_generator, RandomNumber(random_generator, tpcc_district_val_t::MIN_NAME, tpcc_district_val_t::MAX_NAME)).c_str());
      strcpy(district_val.d_street_1,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
      strcpy(district_val.d_street_2,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
      strcpy(district_val.d_city,
             RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
      strcpy(district_val.d_state, RandomStr(random_generator, Address::STATE).c_str());
      strcpy(district_val.d_zip, "123456789");

      district_table_->put(district_key.item_key, &district_val,sizeof(district_val),TSO::get_ts());

    //   total_district_records_inserted += LoadRecord(district_table_,
    //                                                 district_key.item_key,
    //                                                 (void*)&district_val,
    //                                                 sizeof(tpcc_district_val_t),
    //                                                 (table_id_t)TPCCTableType::kDistrictTable,
    //                                                 mem_store_reserve_param);
      total_district_records_examined++;
    }
  }
  // printf("total_district_records_inserted = %d, total_district_records_examined = %d\n",
  //        total_district_records_inserted, total_district_records_examined);
}



// //no batch in this implementation
void TPCC_SCHEMA::PopulateCustomerAndHistoryTable(uint64_t seed) {
  int total_customer_records_inserted = 0, total_customer_records_examined = 0;
  int total_customer_index_records_inserted = 0, total_customer_index_records_examined = 0;
  int total_history_records_inserted = 0, total_history_records_examined = 0;
  // printf("total_customer_records_inserted = %d, total_customer_records_examined = %d\n",
  //        total_customer_records_inserted, total_customer_records_examined);
  FastRandom random_generator(seed);
  printf("num_warehouse_ = %d, num_district_per_warehouse_ = %d, num_customer_per_district_ = %d\n",
         num_warehouse_, num_district_per_warehouse_, num_customer_per_district_);
  for (uint32_t w_id = 1; w_id <= num_warehouse_; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse_; d_id++) {
      for (uint32_t c_id = 1; c_id <= num_customer_per_district_; c_id++) {
        tpcc_customer_key_t customer_key;
        customer_key.c_id = MakeCustomerKey(w_id, d_id, c_id);

        tpcc_customer_val_t customer_val;
        customer_val.c_discount = (float)(RandomNumber(random_generator, 1, 5000) / 10000.0);
        if (RandomNumber(random_generator, 1, 100) <= 10)
          strcpy(customer_val.c_credit, "BC");
        else
          strcpy(customer_val.c_credit, "GC");
        std::string c_last;
        if (c_id <= num_customer_per_district_ / 3) {
          c_last.assign(GetCustomerLastName(random_generator, c_id - 1));
          strcpy(customer_val.c_last, c_last.c_str());
        } else {
          c_last.assign(GetNonUniformCustomerLastNameLoad(random_generator));
          strcpy(customer_val.c_last, c_last.c_str());
        }

        std::string c_first = RandomStr(random_generator, RandomNumber(random_generator, tpcc_customer_val_t::MIN_FIRST, tpcc_customer_val_t::MAX_FIRST));
        strcpy(customer_val.c_first, c_first.c_str());

        customer_val.c_credit_lim = 50000;

        customer_val.c_balance = -10;
        customer_val.c_ytd_payment = 10;
        customer_val.c_payment_cnt = 1;
        customer_val.c_delivery_cnt = 0;
        strcpy(customer_val.c_street_1,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
        strcpy(customer_val.c_street_2,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_STREET, Address::MAX_STREET)).c_str());
        strcpy(customer_val.c_city,
               RandomStr(random_generator, RandomNumber(random_generator, Address::MIN_CITY, Address::MAX_CITY)).c_str());
        strcpy(customer_val.c_state, RandomStr(random_generator, Address::STATE).c_str());
        strcpy(customer_val.c_zip, (RandomNStr(random_generator, 4) + "11111").c_str());

        strcpy(customer_val.c_phone, RandomNStr(random_generator, tpcc_customer_val_t::PHONE).c_str());
        customer_val.c_since = GetCurrentTimeMillis();
        strcpy(customer_val.c_middle, "OE");
        strcpy(customer_val.c_data,
               RandomStr(random_generator, RandomNumber(random_generator, tpcc_customer_val_t::MIN_DATA, tpcc_customer_val_t::MAX_DATA)).c_str());

        assert(!strcmp(customer_val.c_credit, "BC") || !strcmp(customer_val.c_credit, "GC"));
        assert(!strcmp(customer_val.c_middle, "OE"));
        // printf("before insert customer record\n");

        customer_table_->put(customer_key.item_key,(void*)&customer_val,sizeof(tpcc_customer_val_t),TSO::get_ts());
        total_customer_records_examined++;

        // printf("total_customer_records_inserted = %d, total_customer_records_examined = %d\n", total_customer_records_inserted, total_customer_records_examined);

        tpcc_customer_index_key_t customer_index_key;
        //TODO:: MakeCustomerIndexKey may have some problem \
                even the same <w_id, d_id, c_last, c_first> will cause different customer_index_key
        customer_index_key.item_key = MakeCustomerIndexKey(w_id, d_id, c_last, c_first);

        tpcc_customer_index_val_t customer_index_val;
        customer_index_val.debug_magic = tpcc_add_magic;
        // DataItem* mn = GetRecord(customer_index_table_,
        //                          customer_index_key.item_key,
        //                          (table_id_t)TPCCTableType::kCustomerIndexTable);
        //assert(mn == NULL);
        //if (mn == NULL) {
          customer_index_val.c_id = customer_key.c_id;
          customer_table_->put(customer_index_key.item_key,&customer_index_val,sizeof(tpcc_customer_index_val_t),TSO::get_ts());
        //   total_customer_index_records_inserted += LoadRecord(customer_index_table_,
        //                                                       customer_index_key.item_key,
        //                                                       (void*)&customer_index_val,
        //                                                       sizeof(tpcc_customer_index_val_t),
        //                                                       (table_id_t)TPCCTableType::kCustomerIndexTable,
        //                                                       mem_store_reserve_param);
          total_customer_index_records_examined++;
          // printf("total_customer_index_records_inserted = %d, total_customer_index_records_examined = %d\n", total_customer_index_records_inserted, total_customer_index_records_examined);
        //}

        tpcc_history_key_t history_key;
        history_key.h_id = MakeHistoryKey(w_id, d_id, w_id, d_id, c_id);
        tpcc_history_val_t history_val;
        history_val.h_date = GetCurrentTimeMillis();
        history_val.h_amount = 10;
        strcpy(history_val.h_data,
               RandomStr(random_generator, RandomNumber(random_generator, tpcc_history_val_t::MIN_DATA, tpcc_history_val_t::MAX_DATA)).c_str());

        history_table_->put(history_key.item_key,&history_val,sizeof(history_val),TSO::get_ts());
        // total_history_records_inserted += LoadRecord(history_table_,
        //                                              history_key.item_key,
        //                                              (void*)&history_val,
        //                                              sizeof(tpcc_history_val_t),
        //                                              (table_id_t)TPCCTableType::kHistoryTable,
        //                                              mem_store_reserve_param);
        total_history_records_examined++;
        // printf("total_history_records_inserted = %d, total_history_records_examined = %d\n", total_history_records_inserted, total_history_records_examined);
      }
    }
  }

  // printf("total_customer_records_inserted = %d, total_customer_records_examined = %d\n", total_customer_records_inserted, total_customer_records_examined);
  // printf("total_customer_index_records_inserted = %d, total_customer_index_records_examined = %d\n", total_customer_index_records_inserted, total_customer_index_records_examined);
  // printf("total_history_records_inserted = %d, total_history_records_examined = %d\n", total_history_records_inserted, total_history_records_examined);
}

void TPCC_SCHEMA::PopulateOrderNewOrderAndOrderLineTable(uint64_t seed) {
  int total_order_records_inserted = 0, total_order_records_examined = 0;
  int total_order_index_records_inserted = 0, total_order_index_records_examined = 0;
  int total_new_order_records_inserted = 0, total_new_order_records_examined = 0;
  int total_order_line_records_inserted = 0, total_order_line_records_examined = 0;
  FastRandom random_generator(seed);
  // printf("total_order_records_inserted = %d, total_order_records_examined = %d\n", total_order_records_inserted, total_order_records_examined);
  for (uint32_t w_id = 1; w_id <= num_warehouse_; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse_; d_id++) {
      std::set<uint32_t> c_ids_s;
      std::vector<uint32_t> c_ids;
      while (c_ids.size() != num_customer_per_district_) {
        const auto x = (random_generator.Next() % num_customer_per_district_) + 1;
        if (c_ids_s.count(x))
          continue;
        c_ids_s.insert(x);
        c_ids.emplace_back(x);
      }
      for (uint32_t c = 1; c <= num_customer_per_district_; c++) {
        tpcc_order_key_t order_key;
        order_key.o_id = MakeOrderKey(w_id, d_id, c);

        tpcc_order_val_t order_val;
        order_val.o_c_id = c_ids[c - 1];
        if (c <= num_customer_per_district_ * 0.7)
          order_val.o_carrier_id = RandomNumber(random_generator, tpcc_order_val_t::MIN_CARRIER_ID, tpcc_order_val_t::MAX_CARRIER_ID);
        else
          order_val.o_carrier_id = 0;
        order_val.o_ol_cnt = RandomNumber(random_generator, tpcc_order_line_val_t::MIN_OL_CNT, tpcc_order_line_val_t::MAX_OL_CNT);

        order_val.o_all_local = 1;
        order_val.o_entry_d = GetCurrentTimeMillis();

        order_table_->put(order_key.item_key,(void*)&order_val,sizeof(tpcc_order_val_t),TSO::get_ts());
        // total_order_records_inserted += LoadRecord(order_table_,
        //                                            order_key.item_key,
        //                                            (void*)&order_val,
        //                                            sizeof(tpcc_order_val_t),
        //                                            (table_id_t)TPCCTableType::kOrderTable,
        //                                            mem_store_reserve_param);
        total_order_records_examined++;
        // printf("total_order_records_inserted = %d, total_order_records_examined = %d\n", total_order_records_inserted, total_order_records_examined);

        tpcc_order_index_key_t order_index_key;
        order_index_key.item_key = MakeOrderIndexKey(w_id, d_id, order_val.o_c_id, c);

        tpcc_order_index_val_t order_index_val;
        order_index_val.o_id = order_key.o_id;

        // DataItem* mn = GetRecord(order_index_table_,
        //                          order_index_key.item_key,
        //                          (table_id_t)TPCCTableType::kOrderIndexTable);
        // assert(mn == NULL);
        // if (mn == NULL) {
          order_index_val.o_id = order_key.o_id;
          order_index_val.debug_magic = tpcc_add_magic;
          order_index_table_->put(order_index_key.item_key,(void*)&order_index_val,sizeof(tpcc_order_index_val_t),TSO::get_ts());
        //   total_order_index_records_inserted += LoadRecord(order_index_table_,
        //                                                    order_index_key.item_key,
        //                                                    (void*)&order_index_val,
        //                                                    sizeof(tpcc_order_index_val_t),
        //                                                    (table_id_t)TPCCTableType::kOrderIndexTable,
        //                                                    mem_store_reserve_param);
          total_order_index_records_examined++;
          // printf("total_order_index_records_inserted = %d, total_order_index_records_examined = %d\n", total_order_index_records_inserted, total_order_index_records_examined);
        //}

        if (c > num_customer_per_district_ * tpcc_new_order_val_t::SCALE_CONSTANT_BETWEEN_NEWORDER_ORDER) {
          // MZ-Notation: must obey the relationship between the numbers of entries in Order and New-Order specified in tpcc docs
          // The number of entries in New-Order is about 30% of that in Order
          tpcc_new_order_key_t new_order_key;
          new_order_key.no_id = MakeNewOrderKey(w_id, d_id, c);

          tpcc_new_order_val_t new_order_val;
          new_order_val.debug_magic = tpcc_add_magic;
          new_order_table_->put(new_order_key.item_key,(void*)&new_order_val,sizeof(tpcc_new_order_val_t),TSO::get_ts());
        //   total_new_order_records_inserted += LoadRecord(new_order_table_,
        //                                                  new_order_key.item_key,
        //                                                  (void*)&new_order_val,
        //                                                  sizeof(tpcc_new_order_val_t),
        //                                                  (table_id_t)TPCCTableType::kNewOrderTable,
        //                                                  mem_store_reserve_param);
          total_new_order_records_examined++;
          // printf("total_new_order_records_inserted = %d, total_new_order_records_examined = %d\n", total_new_order_records_inserted, total_new_order_records_examined);
        }
        for (uint32_t l = 1; l <= uint32_t(order_val.o_ol_cnt); l++) {
          tpcc_order_line_key_t order_line_key;
          order_line_key.ol_id = MakeOrderLineKey(w_id, d_id, c, l);

          tpcc_order_line_val_t order_line_val;
          order_line_val.ol_i_id = RandomNumber(random_generator, 1, num_item_);
          if (c <= num_customer_per_district_ * 0.7) {
            order_line_val.ol_delivery_d = order_val.o_entry_d;
            order_line_val.ol_amount = 0;
          } else {
            order_line_val.ol_delivery_d = 0;
            /* random within [0.01 .. 9,999.99] */
            order_line_val.ol_amount = (float)(RandomNumber(random_generator, 1, 999999) / 100.0);
          }

          order_line_val.ol_supply_w_id = w_id;
          order_line_val.ol_quantity = 5;
          // order_line_val.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)

          order_line_val.debug_magic = tpcc_add_magic;
          assert(order_line_val.ol_i_id >= 1 && static_cast<size_t>(order_line_val.ol_i_id) <= num_item_);
          order_line_table_->put(order_line_key.item_key,(void*)&order_line_val,sizeof(tpcc_order_line_val_t),TSO::get_ts());
        //   total_order_line_records_inserted += LoadRecord(order_line_table_,
        //                                                   order_line_key.item_key,
        //                                                   (void*)&order_line_val,
        //                                                   sizeof(tpcc_order_line_val_t),
        //                                                   (table_id_t)TPCCTableType::kOrderLineTable,
        //                                                   mem_store_reserve_param);
          total_order_line_records_examined++;
          // printf("total_order_line_records_inserted = %d, total_order_line_records_examined = %d\n", total_order_line_records_inserted, total_order_line_records_examined);
        }
      }
    }
  }
  // printf("total_order_records_inserted = %d, total_order_records_examined = %d\n", total_order_records_inserted, total_order_records_examined);

  //RDMA_LOG(INFO) << "total_order_records_inserted = " << total_order_records_inserted << ", total_order_records_examined = " << total_order_records_examined;

  // printf("total_order_index_records_inserted = %d, total_order_index_records_examined = %d\n",
  //        total_order_index_records_inserted, total_order_index_records_examined);
  // printf("total_new_order_records_inserted = %d, total_new_order_records_examined = %d\n",
  //        total_new_order_records_inserted, total_new_order_records_examined);
  // printf("total_order_line_records_inserted = %d, total_order_line_records_examined = %d\n",
  //        total_order_line_records_inserted, total_order_line_records_examined);
}

// void TPCC_SCHEMA::PopulateItemTable(uint64_t seed) {
//   int total_item_records_inserted = 0, total_item_records_examined = 0;
//   //    printf("total_item_records_inserted = %d, total_item_records_examined = %d\n",
//   //           total_item_records_inserted, total_item_records_examined);
//   FastRandom random_generator(seed);
//   for (int64_t i_id = 1; i_id <= num_item_; i_id++) {
//     tpcc_item_key_t item_key;
//     item_key.i_id = i_id;

//     /* Initialize the item payload */
//     tpcc_item_val_t item_val;

//     strcpy(item_val.i_name,
//            RandomStr(random_generator, RandomNumber(random_generator, tpcc_item_val_t::MIN_NAME, tpcc_item_val_t::MAX_NAME)).c_str());
//     item_val.i_price = (float)(RandomNumber(random_generator, 100, 10000) / 100.0);
//     const int len = RandomNumber(random_generator, tpcc_item_val_t::MIN_DATA, tpcc_item_val_t::MAX_DATA);
//     if (RandomNumber(random_generator, 1, 100) > 10) {
//       strcpy(item_val.i_data, RandomStr(random_generator, len).c_str());
//     } else {
//       const int startOriginal = RandomNumber(random_generator, 2, (len - 8));
//       const std::string i_data = RandomStr(random_generator, startOriginal) +
//                                  "ORIGINAL" + RandomStr(random_generator, len - startOriginal - 8);
//       strcpy(item_val.i_data, i_data.c_str());
//     }
//     item_val.i_im_id = RandomNumber(random_generator, tpcc_item_val_t::MIN_IM, tpcc_item_val_t::MAX_IM);
//     item_val.debug_magic = tpcc_add_magic;
//     //check item price
//     assert(item_val.i_price >= 1.0 && item_val.i_price <= 100.0);

//     total_item_records_inserted += LoadRecord(item_table_,
//                                               item_key.item_key,
//                                               (void*)&item_val,
//                                               sizeof(tpcc_item_val_t),
//                                               (table_id_t)TPCCTableType::kItemTable,
//                                               mem_store_reserve_param);
//     total_item_records_examined++;
//     // printf("total_item_records_inserted = %d, total_item_records_examined = %d\n", total_item_records_inserted, total_item_records_examined);
//   }
//   // printf("total_item_records_inserted = %d, total_item_records_examined = %d\n", total_item_records_inserted, total_item_records_examined);
// }

// void TPCC_SCHEMA::PopulateStockTable(uint64_t seed) {
//   int total_stock_records_inserted = 0, total_stock_records_examined = 0;
//   for (uint32_t w_id = 1; w_id <= num_warehouse_; w_id++) {
//     for (uint32_t i_id = 1; i_id <= num_item_; i_id++) {
//       tpcc_stock_key_t stock_key;
//       stock_key.s_id = MakeStockKey(w_id, i_id);

//       /* Initialize the stock payload */
//       tpcc_stock_val_t stock_val;
//       FastRandom random_generator(seed);
//       stock_val.s_quantity = RandomNumber(random_generator, 10, 100);
//       stock_val.s_ytd = 0;
//       stock_val.s_order_cnt = 0;
//       stock_val.s_remote_cnt = 0;

//       const int len = RandomNumber(random_generator, tpcc_stock_val_t::MIN_DATA, tpcc_stock_val_t::MAX_DATA);
//       if (RandomNumber(random_generator, 1, 100) > 10) {
//         const std::string s_data = RandomStr(random_generator, len);
//         strcpy(stock_val.s_data, s_data.c_str());
//       } else {
//         const int startOriginal = RandomNumber(random_generator, 2, (len - 8));
//         const std::string s_data = RandomStr(random_generator, startOriginal) + "ORIGINAL" + RandomStr(random_generator, len - startOriginal - 8);
//         strcpy(stock_val.s_data, s_data.c_str());
//       }

//       stock_val.debug_magic = tpcc_add_magic;
//       total_stock_records_inserted += LoadRecord(stock_table_,
//                                                  stock_key.item_key,
//                                                  (void*)&stock_val,
//                                                  sizeof(tpcc_stock_val_t),
//                                                  (table_id_t)TPCCTableType::kStockTable,
//                                                  mem_store_reserve_param);
//       total_stock_records_examined++;
//       // printf("total_stock_records_inserted = %d, total_stock_records_examined = %d\n", total_stock_records_inserted, total_stock_records_examined);
//     }
//   }
//   // printf("total_stock_records_inserted = %d, total_stock_records_examined = %d\n", total_stock_records_inserted, total_stock_records_examined);
// }

// int TPCC_SCHEMA::LoadRecord(HashStore* table,
//                      itemkey_t item_key,
//                      void* val_ptr,
//                      size_t val_size,
//                      table_id_t table_id,
//                      MemStoreReserveParam* mem_store_reserve_param) {
//   assert(val_size <= MAX_ITEM_SIZE);
//   /* Insert into HashStore */
//   DataItem item_to_be_inserted(table_id, val_size, item_key, (uint8_t*)val_ptr);
//   DataItem* inserted_item = table->LocalInsert(item_key, item_to_be_inserted, mem_store_reserve_param);
//   inserted_item->remote_offset = table->GetItemRemoteOffset(inserted_item);
//   return 1;
// }

// DataItem* TPCC_SCHEMA::GetRecord(KVEngine* table,
//                           itemkey_t item_key,
//                           table_id_t table_id) {
//   /* get from HashStore */
//   return table->get()
// }
 