#include "tpcc/tpcc_db.h"

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
    
    warehouse_table = new KVEngine();
    PopulateWarehouseTable(9324);

    district_table = new KVEngine();
    PopulateDistrictTable(129856349);

    customer_table = new KVEngine();
    customer_index_table = new KVEngine();
    history_table = new KVEngine();
    PopulateCustomerAndHistoryTable();

    order_table = new KVEngine();
    order_line_table = new KVEngine();
    order_index_table = new KVEngine();
    new_order_table = new KVEngine();
    PopulateOrderNewOrderAndOrderLineTable();

    stock_table = new KVEngine();
    PopulateStockTable();

    item_table = new KVEngine();
    PopulateItemTable();
}

void TPCC_SCHEMA::PopulateWarehouseTable(uint64_t seed){
    int total_warehouse_records_inserted = 0, total_warehouse_records_examined = 0;
    benchmark::FastRandom random_generator(seed);
    for(uint32_t w_id = 1;w_id <= num_warehouse;w_id++){
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
        //warehouse_table->put(warehouse_key, &warehouse_val,sizeof(warehouse_val), TSO::get_ts());
    }
}

void TPCC_SCHEMA::PopulateDistrictTable(uint64_t seed){
    int total_district_records_inserted = 0, total_district_records_examined = 0;
  FastRandom random_generator(seed);
  for (uint32_t w_id = 1; w_id <= num_warehouse; w_id++) {
    for (uint32_t d_id = 1; d_id <= num_district_per_warehouse; d_id++) {
      tpcc_district_key_t district_key;
      district_key.d_id = MakeDistrictKey(w_id, d_id);

      /* Initialize the district payload */
      tpcc_district_val_t district_val;

      district_val.d_ytd = 30000 * 100;  // different from warehouse, notice it did the scale up
      //  NOTICE:: scale should check consistency requirements.
      //  D_YTD = sum(H_AMOUNT) where (D_W_ID, D_ID) = (H_W_ID, H_D_ID).
      district_val.d_tax = (float)RandomNumber(random_generator, 0, 2000) / 10000.0;
      district_val.d_next_o_id = num_customer_per_district + 1;
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

    //   total_district_records_inserted += LoadRecord(district_table,
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

