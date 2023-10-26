#include <memory>
#include "common/benchmark_randomer.h"
#include "dtxn/dtxn.h"
#include "proto/rpc.h"
#include "rrpc/rrpc.h"
#include "micro/micro_db.h"
TxnStatus TxRead(MICRO* micro, Mode mode, bool is_skewed, int index);
TxnStatus TxInsert(MICRO* micro, Mode mode, bool is_skewed, int index);
TxnStatus TxUpdate(MICRO* micro, Mode mode, bool is_skewed, int index);
TxnStatus TxDelete(MICRO* micro, Mode mode, bool is_skewed, int index);