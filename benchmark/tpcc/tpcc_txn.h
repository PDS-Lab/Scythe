#include "config/benchmark_randomer.h"
#include "dtxn/dtxn.h"
#include "proto/rpc.h"
#include "rrpc/rrpc.h"
#include "tpcc/tpcc_db.h"
TxnStatus TxNewOrder(TPCC_SCHEMA* tpcc_client);
TxnStatus TxPayment(TPCC_SCHEMA* tpcc_client);
TxnStatus TxDelivery(TPCC_SCHEMA* tpcc_client);
TxnStatus TxOrderStatus(TPCC_SCHEMA* tpcc_client);
TxnStatus TxStockLevel(TPCC_SCHEMA* tpcc_client);