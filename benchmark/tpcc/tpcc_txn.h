#include "common/benchmark_randomer.h"
#include "dtxn/dtxn.h"
#include "proto/rpc.h"
#include "rrpc/rrpc.h"
#include "tpcc/tpcc_db.h"
TxnStatus TxNewOrder(TPCC_SCHEMA* tpcc_client, Mode mode = Mode::COLD, PhasedLatency* phased_lat = nullptr);
TxnStatus TxPayment(TPCC_SCHEMA* tpcc_client, Mode mode = Mode::COLD, PhasedLatency* phased_lat = nullptr);
TxnStatus TxDelivery(TPCC_SCHEMA* tpcc_client, Mode mode = Mode::COLD, PhasedLatency* phased_lat = nullptr);
TxnStatus TxOrderStatus(TPCC_SCHEMA* tpcc_client, Mode mode = Mode::COLD, PhasedLatency* phased_lat = nullptr);
TxnStatus TxStockLevel(TPCC_SCHEMA* tpcc_client, Mode mode = Mode::COLD, PhasedLatency* phased_lat = nullptr);
TxnStatus TxTestReadWrite();