#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  std::cout << leaf_max_size << " " << internal_max_size << std::endl;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key, Operation op, Transaction *transaction) -> Page * {
  page_id_t next_page_id = root_page_id_;
  Page *prev_page = nullptr;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (op == Operation::Read) {
      page->RLatch();
      if (prev_page == nullptr) {
        root_latch_.RUnlock();
      } else {
        prev_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
      }
    } else {
      page->WLatch();
      if (IsSafePage(tree_page, op)) {
        ReleaseWLatches(transaction);
      }
      transaction->AddIntoPageSet(page);
    }

    if (tree_page->IsLeafPage()) {
      return page;
    }
    auto internal_page = static_cast<InternalPage *>(tree_page);
    int l = 1;
    int r = internal_page->GetSize();
    while (l < r) {
      int mid = (l + r) / 2;
      if (comparator_(internal_page->KeyAt(mid), key) == 1) {
        r = mid;
      } else {
        l = mid + 1;
      }
    }
    next_page_id = internal_page->ValueAt(l - 1);
    prev_page = page;
  }
  return nullptr;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_latch_.RLock();
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return false;
  }
  bool ret = false;
  Page *page = GetLeafPage(key, Operation::Read, transaction);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      ret = true;
      result->push_back(leaf_page->ValueAt(i));
    }
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  root_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::cout << "insert:" << key << std::endl;
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  if (IsEmpty()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    UpdateRootPageId(1);
    auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf_page->SetKeyValueAt(0, key, value);
    leaf_page->IncreaseSize(1);
    leaf_page->SetNextPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    // buffer_pool_manager_->check();

    ReleaseWLatches(transaction);
    return true;
  }

  Page *page = GetLeafPage(key, Operation::Insert, transaction);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      // buffer_pool_manager_->check();
      ReleaseWLatches(transaction);
      return false;
    }
  }

  leaf_page->Insert(key, value, comparator_);
  if (leaf_page->GetSize() < leaf_max_size_) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);

    ReleaseWLatches(transaction);
    // buffer_pool_manager_->check();
    return true;
  }
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf_page->Init(new_page_id, leaf_page->GetParentPageId(), leaf_max_size_);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  leaf_page->MoveDataTo(new_leaf_page, (leaf_max_size_ + 1) / 2, leaf_max_size_ - 1);

  BPlusTreePage *old_tree_page = leaf_page;
  BPlusTreePage *new_tree_page = new_leaf_page;
  KeyType split_key = new_leaf_page->KeyAt(0);
  while (true) {
    if (old_tree_page->IsRootPage()) {
      Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
      auto new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
      new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      new_root_page->SetKeyValueAt(0, split_key, old_tree_page->GetPageId());
      new_root_page->SetKeyValueAt(1, split_key, new_tree_page->GetPageId());
      new_root_page->IncreaseSize(2);
      old_tree_page->SetParentPageId(root_page_id_);
      new_tree_page->SetParentPageId(root_page_id_);
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      break;
    }

    page_id_t parent_page_id = old_tree_page->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_internal_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent_internal_page->Insert(split_key, new_tree_page->GetPageId(), comparator_);
    new_tree_page->SetParentPageId(parent_page_id);
    if (parent_internal_page->GetSize() <= internal_max_size_) {
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      break;
    }

    page_id_t new_internal_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_internal_page_id);
    auto new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_internal_page->Init(new_internal_page_id, parent_internal_page->GetPageId(), internal_max_size_);
    int new_page_size = (internal_max_size_ + 1) / 2;
    int start_index = parent_internal_page->GetSize() - new_page_size;
    for (int i = start_index, j = 0; i < parent_internal_page->GetSize(); i++, j++) {
      new_internal_page->SetKeyValueAt(j, parent_internal_page->KeyAt(i), parent_internal_page->ValueAt(i));
      Page *page = buffer_pool_manager_->FetchPage(parent_internal_page->ValueAt(i));
      auto tree_page = reinterpret_cast<InternalPage *>(page->GetData());
      tree_page->SetParentPageId(new_internal_page_id);
      buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), true);
    }
    parent_internal_page->SetSize(parent_internal_page->GetSize() - new_page_size);
    new_internal_page->SetSize(new_page_size);

    // buffer_pool_manager_->UnpinPage(old_tree_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_tree_page->GetPageId(), true);
    old_tree_page = parent_internal_page;
    new_tree_page = new_internal_page;
    split_key = new_internal_page->KeyAt(0);
  }
  // buffer_pool_manager_->UnpinPage(old_tree_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_tree_page->GetPageId(), true);

  ReleaseWLatches(transaction);
  // buffer_pool_manager_->check();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  if (IsEmpty()) {
    ReleaseWLatches(transaction);
    return;
  }

  std::cout << "remove " << key << std::endl;
  // std::vector<ValueType> results;
  // if (!GetValue(key, &results, transaction)) {
  //   return;
  // }

  Page *page = GetLeafPage(key, Operation::Delete, transaction);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Remove(key, comparator_);
  if (leaf_page->IsRootPage()) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    // buffer_pool_manager_->check();
    ReleaseWLatches(transaction);
    return;
  }
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    HandleUnderFlow(leaf_page, transaction);
  }
  ReleaseWLatches(transaction);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  // buffer_pool_manager_->check();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleUnderFlow(BPlusTreePage *page, Transaction *transaction) {
  if (page->IsRootPage()) {
    if ((page->IsLeafPage() && page->GetSize() == 1) || page->GetSize() > 1) {
      return;
    }

    auto old_root_page = static_cast<InternalPage *>(page);
    root_page_id_ = old_root_page->ValueAt(0);
    auto new_root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId();
    return;
  }

  page_id_t left_sibling_id = INVALID_PAGE_ID;
  page_id_t right_sibling_id = INVALID_PAGE_ID;
  GetSiblings(page, left_sibling_id, right_sibling_id);

  InternalPage *right_page = nullptr;
  InternalPage *left_page = nullptr;
  Page *tmp_page = nullptr;
  if (right_sibling_id != INVALID_PAGE_ID) {
    tmp_page = buffer_pool_manager_->FetchPage(right_sibling_id);
    tmp_page->WLatch();
    right_page = reinterpret_cast<InternalPage *>(tmp_page->GetData());
  } else if (left_sibling_id != INVALID_PAGE_ID) {
    tmp_page = buffer_pool_manager_->FetchPage(right_sibling_id);
    tmp_page->WLatch();
    left_page = reinterpret_cast<InternalPage *>(tmp_page->GetData());
  }
  // std::cout << right_page << " " << left_page << std::endl;
  BUSTUB_ASSERT(right_page != nullptr || left_page != nullptr, "can't find brother page");

  auto parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page->GetParentPageId())->GetData());
  // std::cout<<parent_page<<std::endl;
  if (BorrowKey(page, left_page, parent_page, 1) || BorrowKey(page, right_page, parent_page, 0)) {
    if (left_page != nullptr) {
      tmp_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling_id, true);
    }
    if (right_page != nullptr) {
      tmp_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sibling_id, true);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  if (left_page != nullptr) {
    MergePage(left_page, page, parent_page);
  } else {
    MergePage(page, right_page, parent_page);
  }
  if (left_page != nullptr) {
    tmp_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling_id, true);
  }
  if (right_page != nullptr) {
    tmp_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling_id, true);
  }

  if (parent_page->GetSize() < parent_page->GetMinSize()) {
    HandleUnderFlow(parent_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergePage(BPlusTreePage *left_page, BPlusTreePage *right_page, InternalPage *parent_page) {
  if (left_page->IsLeafPage()) {
    auto left_leaf_page = static_cast<LeafPage *>(left_page);
    auto right_leaf_page = static_cast<LeafPage *>(right_page);
    for (int i = 0; i < right_leaf_page->GetSize(); i++) {
      left_leaf_page->Insert(right_leaf_page->KeyAt(i), right_leaf_page->ValueAt(i), comparator_);
    }  // todo可以直接append在后面
    left_leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
    parent_page->RemoveAt(parent_page->FindValue(right_leaf_page->GetPageId()));
  } else {
    auto left_internal_page = static_cast<InternalPage *>(left_page);
    auto right_internal_page = static_cast<InternalPage *>(right_page);
    left_internal_page->Insert(parent_page->KeyAt(parent_page->FindValue(right_internal_page->GetPageId())),
                               right_internal_page->ValueAt(0), comparator_);
    UpdateParentPageId(right_internal_page->ValueAt(0), left_internal_page->GetPageId());
    parent_page->RemoveAt(parent_page->FindValue(right_internal_page->GetPageId()));
    for (int i = 1; i < right_internal_page->GetSize(); i++) {
      left_internal_page->Insert(right_internal_page->KeyAt(i), right_internal_page->ValueAt(i),
                                 comparator_);  // todo可以直接append在后面
      UpdateParentPageId(right_internal_page->ValueAt(i), left_internal_page->GetPageId());
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentPageId(page_id_t page_id, page_id_t parent_page_id) {
  // std::cout << page_id << "---parent---" << parent_page_id << std::endl;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  auto internal_page = reinterpret_cast<InternalPage *>(page->GetData());
  internal_page->SetParentPageId(parent_page_id);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowKey(BPlusTreePage *page, BPlusTreePage *sibling_page, InternalPage *parent_page,
                               bool is_left) -> bool {
  if (sibling_page == nullptr || sibling_page->GetSize() <= sibling_page->GetMinSize()) {
    return false;
  }

  int sibling_index_at;
  if (is_left) {
    sibling_index_at = sibling_page->GetSize() - 1;
  } else if (sibling_page->IsLeafPage()) {
    sibling_index_at = 0;
  } else {
    sibling_index_at = 1;
  }
  int parent_index_at = parent_page->FindValue(page->GetPageId()) + (is_left ? 0 : 1);
  KeyType update_key;

  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<LeafPage *>(page);
    auto leaf_page_sibling = static_cast<LeafPage *>(sibling_page);
    leaf_page->Insert(leaf_page_sibling->KeyAt(sibling_index_at), leaf_page_sibling->ValueAt(sibling_index_at),
                      comparator_);
    leaf_page_sibling->RemoveAt(sibling_index_at);
    update_key = is_left ? leaf_page->KeyAt(0) : leaf_page_sibling->KeyAt(0);
  } else {
    auto internal_page = static_cast<InternalPage *>(page);
    auto internal_page_sibling = static_cast<InternalPage *>(sibling_page);
    update_key = internal_page_sibling->KeyAt(sibling_index_at);
    page_id_t child_page_id;
    if (is_left) {
      internal_page->Insert(parent_page->KeyAt(parent_index_at), internal_page->ValueAt(0), comparator_);
      internal_page->SetValueAt(0, internal_page_sibling->ValueAt(sibling_index_at));
      child_page_id = internal_page->ValueAt(0);
    } else {
      internal_page->SetKeyValueAt(internal_page->GetSize(), parent_page->KeyAt(parent_index_at),
                                   internal_page_sibling->ValueAt(0));
      internal_page->IncreaseSize(1);
      // internal_page->Insert(parent_page->KeyAt(parent_index_at), internal_page_sibling->ValueAt(sibling_index_at),
      //                       comparator_);
      internal_page_sibling->SetValueAt(0, internal_page_sibling->ValueAt(1));
      child_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    }
    internal_page_sibling->RemoveAt(sibling_index_at);

    Page *page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child_page->SetParentPageId(internal_page->GetPageId());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  parent_page->SetKeyAt(parent_index_at, update_key);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetSiblings(BPlusTreePage *page, page_id_t &left_sibling_id, page_id_t &right_sibling_id) {
  if (page->IsRootPage()) {
    return;
  }
  auto parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page->GetParentPageId())->GetData());

  int index = -1;
  for (int i = 0; i < parent_page->GetSize(); i++) {
    if (parent_page->ValueAt(i) == page->GetPageId()) {
      index = i;
      break;
    }
  }
  BUSTUB_ASSERT(index != -1, "not find page in parent page");

  left_sibling_id = right_sibling_id = INVALID_PAGE_ID;
  if (index > 0) {
    left_sibling_id = parent_page->ValueAt(index - 1);
  }
  if (index < parent_page->GetSize() - 1) {
    right_sibling_id = parent_page->ValueAt(index + 1);
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafePage(BPlusTreePage *tree_page, Operation op) -> bool {
  if (op == Operation::Read) {
    return true;
  }
  if (op == Operation::Insert) {
    if (tree_page->IsLeafPage()) {
      return tree_page->GetSize() < tree_page->GetMaxSize() - 1;
    }
    return tree_page->GetSize() < tree_page->GetMaxSize();
  }
  if (op == Operation::Delete) {
    if (tree_page->IsRootPage()) {
      if (tree_page->IsLeafPage()) {
        return tree_page->GetSize() > 1;
      }
      return tree_page->GetSize() > 2;
    }
    return tree_page->GetMinSize();
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseWLatches(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  std::cout << "release:" << std::endl;
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    Page *page = page_set->front();
    page_set->pop_front();
    if (page == nullptr) {
      root_latch_.WUnlock();
    } else {
      std::cout << page->GetPageId() << " ";
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  std::cout << std::endl;
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  page_id_t next_page_id = root_page_id_;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (tree_page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(tree_page->GetPageId(), buffer_pool_manager_, 0);
    }
    auto internal_page = static_cast<InternalPage *>(tree_page);
    next_page_id = internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Page *page = GetLeafPage(key, Operation::Read, nullptr);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int index = 0;
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      index = i;
      break;
    }
  }
  return INDEXITERATOR_TYPE(page->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, buffer_pool_manager_, 0);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << " size: " << leaf->GetSize() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << " size: " << internal->GetSize() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
