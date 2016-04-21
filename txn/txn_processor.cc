// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)


#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

#include "txn/lock_manager.h"
#include "txn/util.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

int push = 0;
int reqs = 0;
int execute = 0;
int tasksrun = 0;

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  E = 0;
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);
  
  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else {
    storage_ = new Storage();
  }
  
  storage_->InitStorage();

  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  CPU_SET(1, &cpuset);       
  CPU_SET(2, &cpuset);
  CPU_SET(3, &cpuset);
  CPU_SET(4, &cpuset);
  CPU_SET(5, &cpuset);
  CPU_SET(6, &cpuset);  
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));
  
}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;
    
  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  printf("Waiting...\n");
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  printf("RESULT!\n");
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunSiloScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
    case MVCC:                   RunMVCCScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }
          
      if (blocked == false) {
        // Request write locks.
        for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock(); 
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }
      
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));

    }
  }
}

void TxnProcessor::ExecuteTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

void TxnProcessor::RunOCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  RunSerialScheduler();
}

void TxnProcessor::RunOCCParallelScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::RunMVCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute. 
  // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn. 
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::RunSiloScheduler() {
  Txn *txn;

  printf("Silo Scheduler\n");

  tp_.RunTask(new Method <TxnProcessor, void>(
    this,
    &TxnProcessor::EpochManager));

  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      printf("Requests recieved: %i\n", ++reqs);
      tp_.RunTask(new Method <TxnProcessor, void, Txn*>(
        this,
        &TxnProcessor::ExecuteTxnSilo,
        txn)); 
      printf("Tasks run: %i\n", ++tasksrun);
    }
  }
  printf("THREAD POOL INACTIVE\n");
}

void TxnProcessor::EpochManager() {
  printf("Epoch manager\n");
  while(true) {
    sleep(4);
    printf("EPOCH\n");
    ++E;
    Txn *txn;
    while(completed_txns_.Pop(&txn)) {
      // printf("popped\n");
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::ExecuteTxnSilo(Txn *txn) {
  printf("Execute %i\n", ++execute);
  thread_local static uint64 e;
  thread_local static uint64 workerMaxTid; //CHECK does this need to be initialized?
  uint64 maxSeenTid = 0;

  // Read everything in readset
  for (set<Key>::iterator it = txn->readset_.begin(); 
    it != txn->readset_.end(); ++it) {
    Value result;
    if (storage_->Read(*it, &result)) {
      txn->reads_[*it] = result;
      // Keep track of maximum TID seen in read/write set
      maxSeenTid = (UnlockedTid(result.tid) > maxSeenTid) ? 
      UnlockedTid(result.tid) : maxSeenTid;
    }
  }

  // Read everything in writeset
  for (set<Key>::iterator it = txn->writeset_.begin(); 
    it != txn->writeset_.end(); ++it) {
    Value result;
    if (storage_->Read(*it, &result)) {
      txn->reads_[*it] = result;
      // Keep track of maximum TID seen in read/write set
      maxSeenTid = (UnlockedTid(result.tid) > maxSeenTid) ? 
      UnlockedTid(result.tid) : maxSeenTid;
    }
  }

  // printf("RUN\n");
  // Execute transaction logic
  txn->Run(); 

  // printf("LOCKING\n");
  // Lock everything in our writeset in set order
  for (set<Key>::iterator it = txn->writeset_.begin(); 
    it != txn->writeset_.end(); ++it) {
    SiloLock(*it);
  }

  // Remember old epoch number
  uint64 old_e = e;

  // Copy global epoch number
  barrier();
  e = E;
  barrier();

  // printf("VALIDATING\n");

  // Validation
  bool fail;
  for (set<Key>::iterator it = txn->readset_.begin(); 
    it != txn->readset_.end(); ++it) {
    uint64 tid = *(storage_->GetTid(*it));

    // If locked and not locked by us
    if (IsLocked(tid) && (txn->writes_.count(*it) == 0)) {
      fail = true;
      break;
    }
    // If TID changed since our read
    else if (UnlockedTid(tid) != UnlockedTid(txn->reads_[*it].tid)) {
      fail = true;
      break;
    }
  }

  if (fail) {
    Abort(txn);
    // printf("FAILED\n");
    return;
  }
  
  // printf("DIDN'T FAIL\n");
  // Choose txn's TID
  if (old_e != e) {
    workerMaxTid = (e << 51);
  }
  uint64 biggestTid = (workerMaxTid > maxSeenTid) ? workerMaxTid : maxSeenTid;
  // Add 2 to increment starting at the 2nd to last bit, make sure it's unlocked
  uint64 newTid = UnlockedTid(biggestTid + 2);
  //Update worker max tid
  workerMaxTid = newTid;

  ApplySiloWrites(txn, newTid);
  txn->status_ = COMMITTED;

  printf("PUSHED %i\n", ++push);
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplySiloWrites(Txn* txn, uint64 tid) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);

    //Perform memory fence then store TID + release lock
    barrier();
    *(storage_->GetTid(it->first)) = tid;
  }
}

uint64 TxnProcessor::UnlockedTid(uint64 tid) {
  return (tid >> 1) << 1;
}

bool TxnProcessor::IsLocked(uint64 tid) {
  if (tid & 1) {
    return true;
  }
  else {
    return false;
  }
}

void TxnProcessor::SiloLock(Key key) {
  uint64 *tidptr = storage_->GetTid(key);
  uint64 curVal = *tidptr;
  while (!cmp_and_swap(tidptr, UnlockedTid(curVal), curVal | 1)) {
    do_pause();
    curVal = *tidptr;
    printf("DEADLOCK\n");
  }
}

void TxnProcessor::Abort(Txn *txn) {
  txn->status_ = INCOMPLETE;
  txn->reads_.clear();
  txn->writes_.clear();

  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

