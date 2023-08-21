#include <gtest/gtest.h>
#include <cassert>
//at first:simple db
#include "storage/db.h"

#include "util/logging.h"
#include "common/benchmark_randomer.h"
#include "common/types.h"

using benchmark::FastRandom;
using benchmark::zipf_table_distribution;
//商品分布均匀度
static int g_uniform_item_dist = 0;
//新订单中远程商品百分比
static int g_new_order_remote_item_pct = 1;
//仓库微分片数量
static int g_mico_dist_num = 20;
//客户姓氏最大长度
static const size_t CustomerLastNameMaxSize = 16;
//随机姓氏数组
static std::string NameTokens[10] = {
    std::string("BAR"),
    std::string("OUGHT"),
    std::string("ABLE"),
    std::string("PRI"),
    std::string("PRES"),
    std::string("ESE"),
    std::string("ANTI"),
    std::string("CALLY"),
    std::string("ATION"),
    std::string("EING"),
};

const char GOOD_CREDIT[] = "GC";

const char BAD_CREDIT[] = "BC";

static const int DUMMY_SIZE = 12;

static const int DIST = 24;

static const int NUM_DISTRICT_PER_WAREHOUSE = 10;

// Constants
struct Address {
  static const int MIN_STREET = 10;  // W_STREET_1 random a-string [10 .. 20] W_STREET_2 random a-string [10 .. 20]
  static const int MAX_STREET = 20;
  static const int MIN_CITY = 10;  // W_CITY random a-string [10 .. 20]
  static const int MAX_CITY = 20;
  static const int STATE = 2;  // W_STATE random a-string of 2 letters
  static const int ZIP = 9;    // ZIP a-string of 9 letters
};

/******************** TPCC table definitions (Schemas of key and value) start **********************/
/*
 * Warehouse table
 * Primary key: <int32_t w_id>
 */

union tpcc_warehouse_key_t {
  struct {
    int32_t w_id;
    uint8_t unused[4];
  };
  itemkey_t item_key;

  tpcc_warehouse_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_warehouse_key_t) == sizeof(itemkey_t), "");

struct tpcc_warehouse_val_t {
  static const int MIN_NAME = 6;
  static const int MAX_NAME = 10;

  float w_tax;
  float w_ytd;
  char w_name[MAX_NAME + 1];
  char w_street_1[Address::MAX_STREET + 1];
  char w_street_2[Address::MAX_STREET + 1];
  char w_city[Address::MAX_CITY + 1];
  char w_state[Address::STATE + 1];
  char w_zip[Address::ZIP + 1];
};

static_assert(sizeof(tpcc_warehouse_val_t) == 96, "");

/*
 * District table
 * Primary key: <int32_t d_id, int32_t d_w_id>
 */

union tpcc_district_key_t {
  struct {
    int64_t d_id;
  };
  itemkey_t item_key;

  tpcc_district_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_district_key_t) == sizeof(itemkey_t), "");

struct tpcc_district_val_t {
  static const int MIN_NAME = 6;
  static const int MAX_NAME = 10;

  float d_tax;
  float d_ytd;
  int32_t d_next_o_id;
  char d_name[MAX_NAME + 1];
  char d_street_1[Address::MAX_STREET + 1];
  char d_street_2[Address::MAX_STREET + 1];
  char d_city[Address::MAX_CITY + 1];
  char d_state[Address::STATE + 1];
  char d_zip[Address::ZIP + 1];
};

static_assert(sizeof(tpcc_district_val_t) == 100, "");

/*
 * Customer table
 * Primary key: <int32_t c_id, int32_t c_d_id, int32_t c_w_id>
 */

union tpcc_customer_key_t {
  int64_t c_id;
  itemkey_t item_key;

  tpcc_customer_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_customer_key_t) == sizeof(itemkey_t), "");

struct tpcc_customer_val_t {
  static const int MIN_FIRST = 8;  // C_FIRST random a-string [8 .. 16]
  static const int MAX_FIRST = 16;
  static const int MIDDLE = 2;
  static const int MAX_LAST = 16;
  //static const int PHONE = 16;  // C_PHONE random n-string of 16 numbers
  static const int CREDIT = 2;
  static const int MIN_DATA = 300;  // C_DATA random a-string [300 .. 500]
  static const int MAX_DATA = 500;

  float c_credit_lim;
  //float c_discount;
  float c_balance;
  float c_ytd_payment;
  int32_t c_payment_cnt;
  int32_t c_delivery_cnt;
  char c_first[MAX_FIRST + 1];
  char c_middle[MIDDLE + 1];
  char c_last[MAX_LAST + 1];
  //char c_street_1[Address::MAX_STREET + 1];
  //char c_street_2[Address::MAX_STREET + 1];
  //char c_city[Address::MAX_CITY + 1];
  //char c_state[Address::STATE + 1];
  char c_zip[Address::ZIP + 1];
  //char c_phone[PHONE + 1];
  uint32_t c_since;
  char c_credit[CREDIT + 1];
  char c_data[MAX_DATA + 1];
};
//static_assert(sizeof(tpcc_customer_val_t) == 88, "");

union tpcc_customer_index_key_t {
  struct {
    uint64_t c_index_id;
  };
  itemkey_t item_key;

  tpcc_customer_index_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_customer_index_key_t) == sizeof(itemkey_t), "");

struct tpcc_customer_index_val_t {
  int64_t c_id;
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_customer_index_val_t) == 16, "");  // add debug magic

/*
 * History table
 * Primary key: none
 */

union tpcc_history_key_t {
  int64_t h_id;
  itemkey_t item_key;

  tpcc_history_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_history_key_t) == sizeof(itemkey_t), "");

struct tpcc_history_val_t {
  static const int MIN_DATA = 12;  // H_DATA random a-string [12 .. 24] from TPCC documents 5.11
  static const int MAX_DATA = 24;

  float h_amount;
  uint32_t h_date;
  char h_data[MAX_DATA + 1];
};

static_assert(sizeof(tpcc_history_val_t) == 36, "");

/*
 * NewOrder table
 * Primary key: <int32_t no_w_id, int32_t no_d_id, int32_t no_o_id>
 */
union tpcc_new_order_key_t {
  int64_t no_id;
  itemkey_t item_key;

  tpcc_new_order_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_new_order_key_t) == sizeof(itemkey_t), "");

struct tpcc_new_order_val_t {
  static constexpr double SCALE_CONSTANT_BETWEEN_NEWORDER_ORDER = 0.7;

  char no_dummy[DUMMY_SIZE + 1];
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_new_order_val_t) == 24, "");  // add debug magic
// static_assert(sizeof(tpcc_new_order_val_t) == 13, "");

/*
 * Order table
 * Primary key: <int32_t o_w_id, int32_t o_d_id, int32_t o_id>
 */
union tpcc_order_key_t {
  int64_t o_id;
  itemkey_t item_key;

  tpcc_order_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_key_t) == sizeof(itemkey_t), "");

struct tpcc_order_val_t {
  static const int MIN_CARRIER_ID = 1;
  static const int MAX_CARRIER_ID = 10;  // number of distinct per warehouse

  int32_t o_c_id;
  int32_t o_carrier_id;
  int32_t o_ol_cnt;
  int32_t o_all_local;
  uint32_t o_entry_d;
};

static_assert(sizeof(tpcc_order_val_t) == 20, "");

union tpcc_order_index_key_t {
  int64_t o_index_id;
  itemkey_t item_key;

  tpcc_order_index_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_index_key_t) == sizeof(itemkey_t), "");

struct tpcc_order_index_val_t {
  uint64_t o_id;
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_order_index_val_t) == 16, "");  // add debug magic
// static_assert(sizeof(tpcc_order_index_val_t) == 8, "");

/*
 * OrderLine table
 * Primary key: <int32_t ol_o_id, int32_t ol_d_id, int32_t ol_w_id, int32_t ol_number>
 */

union tpcc_order_line_key_t {
  int64_t ol_id;
  itemkey_t item_key;

  tpcc_order_line_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_order_line_key_t) == sizeof(itemkey_t), "");

struct tpcc_order_line_val_t {
  static const int MIN_OL_CNT = 5;
  static const int MAX_OL_CNT = 15;

  int32_t ol_i_id;
  int32_t ol_supply_w_id;
  int32_t ol_quantity;
  float ol_amount;
  uint32_t ol_delivery_d;
  char ol_dist_info[DIST + 1];
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_order_line_val_t) == 56, "");  // add debug magic
// static_assert(sizeof(tpcc_order_line_val_t) == 48, "");

/*
 * Item table
 * Primary key: <int32_t i_id>
 */

union tpcc_item_key_t {
  struct {
    int64_t i_id;
  };
  itemkey_t item_key;

  tpcc_item_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_item_key_t) == sizeof(itemkey_t), "");

struct tpcc_item_val_t {
  static const int MIN_NAME = 14;  // I_NAME random a-string [14 .. 24]
  static const int MAX_NAME = 24;
  static const int MIN_DATA = 26;
  static const int MAX_DATA = 50;  // I_DATA random a-string [26 .. 50]

  static const int MIN_IM = 1;
  static const int MAX_IM = 10000;

  int32_t i_im_id;
  float i_price;
  char i_name[MAX_NAME + 1];
  char i_data[MAX_DATA + 1];
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_item_val_t) == 96, "");  // add debug magic
// static_assert(sizeof(tpcc_item_val_t) == 84, "");

/*
 * Stock table
 * Primary key: <int32_t s_i_id, int32_t s_w_id>
 */

union tpcc_stock_key_t {
  struct {
    int64_t s_id;
  };
  itemkey_t item_key;

  tpcc_stock_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(tpcc_stock_key_t) == sizeof(itemkey_t), "");

struct tpcc_stock_val_t {
  static const int MIN_DATA = 26;
  static const int MAX_DATA = 50;
  static const int32_t MIN_STOCK_LEVEL_THRESHOLD = 10;
  static const int32_t MAX_STOCK_LEVEL_THRESHOLD = 20;
  static const int STOCK_LEVEL_ORDERS = 20;

  int32_t s_quantity;
  int32_t s_ytd;
  int32_t s_order_cnt;
  int32_t s_remote_cnt;
  char s_dist[NUM_DISTRICT_PER_WAREHOUSE][DIST + 1];
  char s_data[MAX_DATA + 1];
  int64_t debug_magic;
};

static_assert(sizeof(tpcc_stock_val_t) == 328, "");  // add debug magic
// static_assert(sizeof(tpcc_stock_val_t) == 320, "");

const std::string tpcc_zip_magic("123456789");  // warehouse, district
const uint32_t tpcc_no_time_magic = 0;          // customer, history, order
const int64_t tpcc_add_magic = 818;             // customer_index, order_index, new_order, order_line, item, stock

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_NEW_ORDER 45
#define FREQUENCY_PAYMENT 43
#define FREQUENCY_ORDER_STATUS 4
#define FREQUENCY_DELIVERY 4
#define FREQUENCY_STOCK_LEVEL 4

// Transaction workload type
#define TPCC_TX_TYPES 5
enum class TPCCTxType {
  kNewOrder = 0,
  kPayment,
  kDelivery,
  kOrderStatus,
  kStockLevel,
};

const std::string TPCC_TX_NAME[TPCC_TX_TYPES] = {"NewOrder", "Payment", "Delivery", \
"OrderStatus", "StockLevel"};

// Table id
enum class TPCCTableType : uint32_t {
  kWarehouseTable = 1,
  kDistrictTable,
  kCustomerTable,
  kHistoryTable,
  kNewOrderTable,
  kOrderTable,
  kOrderLineTable,
  kItemTable,
  kStockTable,
  kCustomerIndexTable,
  kOrderIndexTable,
  TableNum
};

class TPCC_SCHEMA{
private:
public:
    std::string bench_name_;

    // Pre-defined constants, which will be modified for tests
    uint32_t num_warehouse_ = 10;

    uint32_t num_district_per_warehouse_ = 10;

    uint32_t num_customer_per_district_ = 3000;

    uint32_t num_item_ = 100000;

    uint32_t num_stock_per_warehouse_ = 100000;

    // /* Tables */ 等待后续基于内存重构db
    KVEngine* warehouse_table_ = nullptr;

    KVEngine* district_table_ = nullptr;

    KVEngine* customer_table_ = nullptr;

    KVEngine* history_table_ = nullptr;

    KVEngine* new_order_table_ = nullptr;

    KVEngine* order_table_ = nullptr;

    KVEngine* order_line_table_ = nullptr;

    KVEngine* item_table_ = nullptr;

    KVEngine* stock_table_ = nullptr;

    KVEngine* customer_index_table_ = nullptr;

    KVEngine* order_index_table_ = nullptr;

    TPCCTxType workgen_arr_[100];

    TPCCTxType* workload_arr_ = nullptr;

    FastRandom* f_rand_ = nullptr;
    
    //是否要做三备份数据节点，暂时不需要
    //std::vector<HashStore*> primary_table_ptrs;

    //std::vector<HashStore*> backup_table_ptrs;

    TPCC_SCHEMA(
                uint32_t num_warehouse = 10,
                uint32_t num_district_per_warehouse = 10,
                uint32_t num_customer_per_district = 3000,
                uint32_t num_item = 100000,
                uint32_t num_stock_per_warehouse = 100000):
                num_warehouse_(num_warehouse),
                num_district_per_warehouse_(num_district_per_warehouse),
                num_customer_per_district_(num_customer_per_district),
                num_item_(num_item),
                num_stock_per_warehouse_(num_stock_per_warehouse)
                {
        bench_name_ = "TPCC";
    }
    ~TPCC_SCHEMA(){
        if(warehouse_table_) delete warehouse_table_;
        if(customer_table_) delete customer_table_;
        if(history_table_) delete history_table_;
        if(new_order_table_) delete new_order_table_;
        if(order_table_) delete order_table_;
        if(order_line_table_) delete order_line_table_;
        if(item_table_) delete item_table_;
        if(stock_table_) delete stock_table_;
        if(workload_arr_) delete[] workload_arr_;
    }
    void CreateWorkgenArray();
    void CreateWorkLoad(int task_num);
    void LoadTable();
    void PopulateWarehouseTable(uint64_t seed);
    void PopulateDistrictTable(uint64_t seed);
    void PopulateCustomerAndHistoryTable(uint64_t seed);
    void PopulateOrderNewOrderAndOrderLineTable(uint64_t seed);
    void PopulateItemTable(uint64_t seed);
    void PopulateStockTable(uint64_t seed);
     
  uint32_t GetCurrentTimeMillis() {
    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number
    static __thread uint32_t tl_hack = 0;
    return ++tl_hack;
  }

  // utils for generating random #s and strings
   
  int CheckBetweenInclusive(int v, int lower, int upper) {
    assert(v >= lower);
    assert(v <= upper);
    return v;
  }

   
  int RandomNumber(FastRandom& r, int min, int max) {
    return CheckBetweenInclusive((int)(r.NextUniform() * (max - min + 1) + min), min, max);
  }

   
  int NonUniformRandom(FastRandom& r, int A, int C, int min, int max) {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
  }

   
  int64_t GetItemId(FastRandom& r) {
    return CheckBetweenInclusive(g_uniform_item_dist ? RandomNumber(r, 1, num_item_) : NonUniformRandom(r, 8191, 7911, 1, num_item_), 1, num_item_);
  }

   
  int GetCustomerId(FastRandom& r) {
    return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, num_customer_per_district_), 1, num_customer_per_district_);
  }

  // pick a number between [start, end)
   
  unsigned PickWarehouseId(FastRandom& r, unsigned start, unsigned end) {
    assert(start < end);
    const unsigned diff = end - start;
    if (diff == 1)
      return start;
    return (r.Next() % diff) + start;
  }

  inline size_t GetCustomerLastName(uint8_t* buf, FastRandom& r, int num) {
    const std::string& s0 = NameTokens[num / 100];
    const std::string& s1 = NameTokens[(num / 10) % 10];
    const std::string& s2 = NameTokens[num % 10];
    uint8_t* const begin = buf;
    const size_t s0_sz = s0.size();
    const size_t s1_sz = s1.size();
    const size_t s2_sz = s2.size();
    memcpy(buf, s0.data(), s0_sz);
    buf += s0_sz;
    memcpy(buf, s1.data(), s1_sz);
    buf += s1_sz;
    memcpy(buf, s2.data(), s2_sz);
    buf += s2_sz;
    return buf - begin;
  }

   
  size_t GetCustomerLastName(char* buf, FastRandom& r, int num) {
    return GetCustomerLastName((uint8_t*)buf, r, num);
  }

  inline std::string GetCustomerLastName(FastRandom& r, int num) {
    std::string ret;
    ret.resize(CustomerLastNameMaxSize);
    ret.resize(GetCustomerLastName((uint8_t*)&ret[0], r, num));
    return ret;
  }

   
  std::string GetNonUniformCustomerLastNameLoad(FastRandom& r) {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
  }

   
  size_t GetNonUniformCustomerLastNameRun(uint8_t* buf, FastRandom& r) {
    return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
  }

   
  size_t GetNonUniformCustomerLastNameRun(char* buf, FastRandom& r) {
    return GetNonUniformCustomerLastNameRun((uint8_t*)buf, r);
  }

   
  std::string GetNonUniformCustomerLastNameRun(FastRandom& r) {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
  }

   
  std::string RandomStr(FastRandom& r, uint64_t len) {
    // this is a property of the oltpbench implementation...
    if (!len)
      return "";

    uint64_t i = 0;
    std::string buf(len, 0);
    while (i < (len)) {
      const char c = (char)r.NextChar();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c))
        continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a string of length len
   
  std::string RandomNStr(FastRandom& r, uint64_t len) {
    const char base = '0';
    std::string buf(len, 0);
    for (uint64_t i = 0; i < len; i++)
      buf[i] = (char)(base + (r.Next() % 10));
    return buf;
  }

   
  int64_t MakeDistrictKey(int32_t w_id, int32_t d_id) {
    int32_t did = d_id + (w_id * num_district_per_warehouse_);
    int64_t id = static_cast<int64_t>(did);
    // assert(districtKeyToWare(id) == w_id);
    return id;
  }

   
  int64_t MakeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    int32_t upper_id = w_id * num_district_per_warehouse_ + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(c_id);
    // assert(customerKeyToWare(id) == w_id);
    return id;
  }

  // only used for customer index, maybe some problems when used.
   
  void ConvertString(char* newstring, const char* oldstring, int size) {
    for (int i = 0; i < 8; i++)
      if (i < size)
        newstring[7 - i] = oldstring[i];
      else
        newstring[7 - i] = '\0';

    for (int i = 8; i < 16; i++)
      if (i < size)
        newstring[23 - i] = oldstring[i];
      else
        newstring[23 - i] = '\0';
  }

   
  uint64_t MakeCustomerIndexKey(int32_t w_id, int32_t d_id, std::string s_last, std::string s_first) {
    uint64_t* seckey = new uint64_t[5];
    int32_t did = d_id + (w_id * num_district_per_warehouse_);
    seckey[0] = did;
    ConvertString((char*)(&seckey[1]), s_last.data(), s_last.size());
    ConvertString((char*)(&seckey[3]), s_first.data(), s_first.size());
    return (uint64_t)seckey;
  }

   
  int64_t MakeHistoryKey(int32_t h_w_id, int32_t h_d_id, int32_t h_c_w_id, int32_t h_c_d_id, int32_t h_c_id) {
    int32_t cid = (h_c_w_id * num_district_per_warehouse_ + h_c_d_id) * num_customer_per_district_ + h_c_id;
    int32_t did = h_d_id + (h_w_id * num_district_per_warehouse_);
    int64_t id = static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
    return id;
  }

   
  int64_t MakeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    int32_t upper_id = w_id * num_district_per_warehouse_ + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    return id;
  }

   
  int64_t MakeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    int32_t upper_id = w_id * num_district_per_warehouse_ + d_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    // assert(orderKeyToWare(id) == w_id);
    return id;
  }

   
  int64_t MakeOrderIndexKey(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
    int32_t upper_id = (w_id * num_district_per_warehouse_ + d_id) * num_customer_per_district_ + c_id;
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
    return id;
  }

   
  int64_t MakeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
    int32_t upper_id = w_id * num_district_per_warehouse_ + d_id;
    // 10000000 is the MAX ORDER ID
    int64_t oid = static_cast<int64_t>(upper_id) * 10000000 + static_cast<int64_t>(o_id);
    int64_t olid = oid * 15 + number;
    int64_t id = static_cast<int64_t>(olid);
    // assert(orderLineKeyToWare(id) == w_id);
    return id;
  }

  int64_t MakeStockKey(int32_t w_id, int32_t i_id) {
    int32_t item_id = i_id + (w_id * num_stock_per_warehouse_);
    int64_t s_id = static_cast<int64_t>(item_id);
    // assert(stockKeyToWare(id) == w_id);
    return s_id;
  }

};