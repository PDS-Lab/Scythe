#pragma once

#include <memory>
#include "config/benchmark_randomer.h"
#include "dtxn/dtxn.h"
#include "proto/rpc.h"
#include "rrpc/rrpc.h"
#include "smallbank/smallbank_db.h"
/******************** The business logic (Transaction) start ********************/
TxnStatus TxAmalgamate(SmallBank* smallbank, Mode mode);
/* Calculate the sum of saving and checking kBalance */
TxnStatus TxBalance(SmallBank* smallbank, Mode mode);
/* Add $1.3 to acct_id's checking account */
TxnStatus TxDepositChecking(SmallBank* smallbank, Mode mode);
/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
TxnStatus TxSendPayment(SmallBank* smallbank, Mode mode);
/* Add $20 to acct_id's saving's account */
TxnStatus TxTransactSaving(SmallBank* smallbank, Mode mode);
/* Read saving and checking kBalance + update checking kBalance unconditionally */
TxnStatus TxWriteCheck(SmallBank* smallbank, Mode mode);
/******************** The business logic (Transaction) end ********************/

TxnStatus SbTxTestReadWrite();