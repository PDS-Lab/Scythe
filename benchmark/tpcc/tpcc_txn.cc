#include "tpcc_txn.h"
#include "coroutine_pool/coroutine_pool.h"
TxnStatus TxNewOrder(TPCC_SCHEMA* tpcc){
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
    auto txn = TransactionFactory::TxnBegin(Mode::COLD, (uint32_t)TPCCTableType::TableNum);
    
    int warehouse_id_start_ = 1;
    int warehouse_id_end_ = tpcc->num_warehouse_;

    int district_id_start = 1;
    int district_id_end_ = tpcc->num_district_per_warehouse_;
    const uint32_t warehouse_id = tpcc->PickWarehouseId(random_generator[dtx->coro_id], warehouse_id_start_, warehouse_id_end_);
    const uint32_t district_id = tpcc->RandomNumber(random_generator[dtx->coro_id], district_id_start, district_id_end_);
    const uint32_t customer_id = tpcc->GetCustomerId(random_generator[dtx->coro_id]);
    int64_t c_key = tpcc->MakeCustomerKey(warehouse_id, district_id, customer_id);
    
    //涉及到的商品有99%是本仓库，另外1%远程调货
    
    int32_t all_local = 1;
    std::set<uint64_t> stock_set;  // remove identity stock ids;

    // local buffer used store stocks
    int64_t remote_stocks[tpcc_order_line_val_t::MAX_OL_CNT], local_stocks[tpcc_order_line_val_t::MAX_OL_CNT];
    int64_t remote_item_ids[tpcc_order_line_val_t::MAX_OL_CNT], local_item_ids[tpcc_order_line_val_t::MAX_OL_CNT];
    uint32_t local_supplies[tpcc_order_line_val_t::MAX_OL_CNT], remote_supplies[tpcc_order_line_val_t::MAX_OL_CNT];

    int num_remote_stocks(0), num_local_stocks(0);

    const int num_items = tpcc->RandomNumber(random_generator[dtx->coro_id], tpcc_order_line_val_t::MIN_OL_CNT, tpcc_order_line_val_t::MAX_OL_CNT);

    for (int i = 0; i < num_items; i++) {
      int64_t item_id = tpcc->GetItemId(random_generator[dtx->coro_id]);
      if (tpcc->num_warehouse_ == 1 ||
          tpcc->RandomNumber(random_generator[dtx->coro_id], 1, 100) > g_new_order_remote_item_pct) {
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
              tpcc->RandomNumber(random_generator[dtx->coro_id], 1, tpcc->num_warehouse_);
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
    //step1: get*3
    tpcc_warehouse_key_t ware_key;
    ware_key.w_id = warehouse_id;
    auto ware_obj = txn->GetObject(ware_key.item_key,(uint32_t)TPCCTableType::kWarehouseTable, sizeof(tpcc_warehouse_val_t));
    txn->Read(ware_obj);
    //getCustomer
    tpcc_customer_key_t cust_key;
    cust_key.c_id = c_key;
    auto cust_obj = txn->GetObject(cust_key.item_key,(uint32_t)TPCCTableType::kCustomerTable,sizeof(tpcc_customer_val_t));
    txn->Read(cust_obj);
    //getDistrict
    uint64_t dkey = tpcc->MakeDistrictKey(warehouse_id,district_id);
    tpcc_district_key_t dist_key;
    dist_key.d_id = dkey;
    auto dist_obj = txn->GetObject(dist_key.item_key,(uint32_t)TPCCTableType::kDistrictTable,sizeof(tpcc_district_val_t));
    txn->Read(dist_obj);
    //check
    /*
    {
        auto ware_val = ware_obj->get_as<tpcc_warehouse_val_t>();
        std::string check(ware_val->w_zip);
        if (check != tpcc_zip_magic) {
            LOG_FATAL("[FATAL] Read warehouse unmatch, tid-cid-txid");
        }

        auto cust_val = cust_obj->get_as<tpcc_customer_val_t>();
        // c_since never be 0
        if (cust_val->c_since == 0) {
            LOG_FATAL("[FATAL] Read customer unmatch, tid-cid-txid");
        }

        auto dist_val = dist_obj->get_as<tpcc_district_val_t>();
        check = std::string(dist_val->d_zip);
        if (check != tpcc_zip_magic) {
            LOG_FATAL("[FATAL] Read district unmatch, tid-cid-txid");
        }
    }
    */
    //incrementNextOrderId
    auto dist_val = dist_obj->get_as<tpcc_district_val_t>();
    auto my_next_o_id = dist_val->d_next_o_id++;
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
    norder_obj->set_new();
    auto order_val = order_obj->get_as<tpcc_order_val_t>();
    order_val->o_c_id = int32_t(customer_id);
    order_val->o_carrier_id = 0;
    order_val->o_ol_cnt = num_items;
    order_val->o_all_local = all_local;
    order_val->o_entry_d = tpcc->GetCurrentTimeMillis();
    txn->Write(norder_obj);

    //whether to do: index table 

    //getItemInfo
    //getStockInfo
    //updateStock
    //createOrderLine
    for (int ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
      const int64_t ol_i_id = local_item_ids[ol_number - 1];
      const uint32_t ol_quantity = tpcc->RandomNumber(random_generator[dtx->coro_id], 1, 10);
      // read item info
      tpcc_item_key_t tpcc_item_key;
      tpcc_item_key.i_id = ol_i_id;

      auto item_obj = txn->GetObject(tpcc_item_key.item_key,(uint32_t)TPCCTableType::kItemTable,sizeof(tpcc_item_val_t));
      txn->Read(item_obj);

      int64_t s_key = local_stocks[ol_number - 1];
      // read and update stock info
      tpcc_stock_key_t stock_key;
      stock_key.s_id = s_key;

      auto stock_obj = txn->GetObject(stock_key.item_key,(uint32_t)TPCCTableType::kStockTable,sizeof(tpcc_stock_val_t));
      txn->Read(stock_obj);

      tpcc_item_val_t* item_val = item_obj->get_as<tpcc_item_val_t>();
      tpcc_stock_val_t* stock_val = stock_obj->get_as<tpcc_stock_val_t>();

      // if (item_val->debug_magic != tpcc_add_magic) {
      //   RDMA_LOG(FATAL) << "[FATAL] Read item unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
      // }
      // if (stock_val->debug_magic != tpcc_add_magic) {
      //   RDMA_LOG(FATAL) << "[FATAL] Read stock unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
      // }

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
      txn->Read(ol_obj);
      //   if (!dtx->TxExe(yield)) return false;

      tpcc_order_line_val_t* order_line_val = ol_obj->get_as<tpcc_order_line_val_t>();

      order_line_val->ol_i_id = int32_t(ol_i_id);
      order_line_val->ol_delivery_d = 0;  // not delivered yet
      order_line_val->ol_amount = float(ol_quantity) * item_val->i_price;
      order_line_val->ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
      order_line_val->ol_quantity = int8_t(ol_quantity);
      order_line_val->debug_magic = tpcc_add_magic;
      txn->Write(ol_obj);
    }
    auto rc = txn->Commit();
    return rc;    
}
TxnStatus TxPayment(TPCC_SCHEMA* tpcc){
    // auto txn = TransactionFactory::TxnBegin();
    // int x = tpcc->RandomNumber(random_generator[dtx->coro_id], 1, 100);
    // int y = tpcc->RandomNumber(random_generator[dtx->coro_id], 1, 100);

    // int warehouse_id_start_ = 1;
    // int warehouse_id_end_ = tpcc->num_warehouse_;

    // int district_id_start = 1;
    // int district_id_end_ = tpcc->num_district_per_warehouse_;

    // const uint32_t warehouse_id = tpcc->PickWarehouseId(random_generator[dtx->coro_id], warehouse_id_start_, warehouse_id_end_);
    // const uint32_t district_id = tpcc->RandomNumber(random_generator[dtx->coro_id], district_id_start, district_id_end_);

    // int32_t c_w_id;
    // int32_t c_d_id;
    
    return TxnStatus::OK;
}
TxnStatus TxDelivery(TPCC_SCHEMA* tpcc){
    auto txn = TransactionFactory::TxnBegin();
    return TxnStatus::OK;
}
TxnStatus TxOrderStatus(TPCC_SCHEMA* tpcc){
    auto txn = TransactionFactory::TxnBegin();
    return TxnStatus::OK;
}
TxnStatus TxStockLevel(TPCC_SCHEMA* tpcc){
    auto txn = TransactionFactory::TxnBegin();
    return TxnStatus::OK;
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
    strncpy(ware_val->w_zip,"987654321\0",10);
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