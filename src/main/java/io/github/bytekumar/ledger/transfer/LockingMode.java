package io.github.bytekumar.ledger.transfer;

/** How a transfer protects the two balances it changes. Set with ledger.transfer.locking. */
public enum LockingMode {
    /** Read without locks, then UPDATE ... WHERE version = ?; on a lost race, roll back and retry. */
    OPTIMISTIC,
    /** SELECT ... FOR UPDATE both accounts first (lower id first); others wait instead of retrying. */
    PESSIMISTIC
}
