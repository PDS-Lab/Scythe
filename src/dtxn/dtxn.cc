#include "dtxn.h"

#include <memory>

#include "coroutine_pool/scheduler.h"
#include "impl/occ.h"
#include "impl/toc.h"
#include "util/logging.h"

std::shared_ptr<Transaction> TransactionFactory::TxnBegin(Mode mode, uint32_t table_num) {
  LOG_ASSERT(this_coroutine::is_coro_env(), "not in coroutine environment");
  switch (mode) {
    case Mode::COLD:
      return std::shared_ptr<OCC>(new OCC(table_num));
    case Mode::HOT:
      return std::shared_ptr<TOC>(new TOC(table_num));
  }
  return nullptr;
}