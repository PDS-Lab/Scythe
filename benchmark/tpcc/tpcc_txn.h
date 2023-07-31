#include "config/benchmark_randomer.h"
#include "dtxn/dtxn.h"
#include "proto/rpc.h"
#include "rrpc/rrpc.h"
#include "tpcc/tpcc_db.h"
void TxNewOrder(TPCC_SCHEMA* tpcc_client);
void TxPayment(TPCC_SCHEMA* tpcc_client);
void TxDelivery(TPCC_SCHEMA* tpcc_client);
void TxOrderStatus(TPCC_SCHEMA* tpcc_client);
void TxStockLevel(TPCC_SCHEMA* tpcc_client);