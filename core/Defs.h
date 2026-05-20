//
// Created by Yi Lu on 9/10/18.
//

#pragma once

namespace aria {

enum class ExecutorStatus {
  START,
  CLEANUP,
  C_PHASE,
  S_PHASE,
  Analysis,
  Execute,
  Aria_READ,
  Aria_COMMIT,
  AriaFB_READ,
  AriaFB_COMMIT,
  AriaFB_Fallback_Prepare,
  AriaFB_Fallback,
  PRODCC_PARTITION,
  PRODCC_GENERATE_MACRO_BLOCK,
  PRODCC_EXECUTE_MICRO_BLOCK,
  PRODCC_RESERVE_MICRO_BLOCK,
  PRODCC_ANALYZE_MICRO_BLOCK,
  PRODCC_FINALIZE_MICRO_BLOCK,
  PRODCC_CLEANUP_MICRO_BLOCK,
  Bohm_Analysis,
  Bohm_Insert,
  Bohm_Execute,
  Bohm_GC,
  Pwv_Analysis,
  Pwv_Execute,
  DOCC_EXECUTE,
  DOCC_COMMIT,
  STOP,
  EXIT
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

} // namespace aria
