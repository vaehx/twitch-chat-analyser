package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public abstract class XAPostgresSink<T> extends TwoPhaseCommitSinkFunction<T, String, Void> {

    private final static Logger LOG = LoggerFactory.getLogger(XAPostgresSink.class);

    private transient Connection conn;
    private transient PreparedStatement preparedStmt;
    private transient int inCurrentBatch;
    private transient ReentrantLock hasPendingCommitTxLock;

    private String jdbcUrl;
    private int batchSize;


    public XAPostgresSink(String jdbcUrl, int batchSize) {
        super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);

        this.jdbcUrl = jdbcUrl;
        this.batchSize = batchSize;
    }

    /**
     * Returns the SQL for the prepared insert statement
     */
    protected abstract String getInsertSQL();

    /**
     * Sets all parameters for the prepared statement for the element, as defined by the
     * statement returned in {@link XAPostgresSink#getInsertSQL()}.
     *
     * This is called for each element and should neither modify the element nor the statement.
     */
    protected abstract void setFields(PreparedStatement stmt, T t) throws SQLException;


    @Override
    public void open(Configuration configuration) throws Exception {
        if (!isConnected())
            reconnect();

        super.open(configuration);
    }

    private boolean isConnected() {
        try {
            return conn != null && !conn.isClosed();
        } catch (Exception ex) {
            conn = null;
            return false;
        }
    }

    private void reconnect() throws Exception {
        String insertSql = getInsertSQL();
        if (insertSql == null || insertSql.isEmpty())
            throw new IllegalArgumentException("No insert SQL provided!");

        Class.forName("org.postgresql.Driver");

        conn = DriverManager.getConnection(jdbcUrl);
        conn.setAutoCommit(false);

        preparedStmt = conn.prepareStatement(insertSql);

        LOG.info("(Re-)connected to Database. Using max batch size of {}", batchSize);

        // Reset lock for new connection
        hasPendingCommitTxLock = new ReentrantLock();

        inCurrentBatch = 0;
    }

    /**
     * Begins a new transaction, i.e. generates a new TXID after the previous TX was preCommitted.
     */
    @Override
    protected String beginTransaction() throws Exception {
        if (!isConnected())
            reconnect();

        // Transaction is automatically started by JDBC with the first executed statement, since auto-commit is off

        return UUID.randomUUID().toString();
    }

    @Override
    protected void invoke(String txid, T t, Context context) throws Exception {
        setFields(preparedStmt, t);
        preparedStmt.addBatch();
        inCurrentBatch++;

        if (inCurrentBatch >= batchSize) {
            // Only flush batch if the lock isn't taken by an async snapshot-thread. Otherwise, ignore flush
            // for now. The batch will at last be flushed in preCommit().
            // IMPORTANT: The snapshot-thread may even be the current thread and invoke() be called on this thread
            // after preCommit() but before commit(). In this case, we must not issue any execute of the current
            // batch here either, because the prepared transaction may hold a RowExclusiveLock in Postgres on the
            // same row that is getting updated in the current batch. If we ignored that, we may cause a deadlock
            // in which Postgres waits for our Flink client thread to commit the prepared Transaction, but our thread
            // also waits for a lock on the same row, held by said transaction.
            if (!hasPendingCommitTxLock.isHeldByCurrentThread() && hasPendingCommitTxLock.tryLock()) {
                preparedStmt.executeBatch();
                inCurrentBatch = 0;
                hasPendingCommitTxLock.unlock();
            }
        }
    }

    @Override
    protected void preCommit(String txid) throws Exception {
        // In case of async snapshots, prevent processing thread to flush until this transaction is commit()-ed.
        // This is, because Postgres Prepared Transactions keep holding their locks on data until they're either
        // rolled back or committed.
        hasPendingCommitTxLock.lock();

        // Submit remaining incomplete batch
        if (inCurrentBatch > 0) {
            preparedStmt.executeBatch();
            inCurrentBatch = 0;
        }

        // Prepare current transaction for later commit or rollback
        try (Statement txStmt = conn.createStatement()) {
            txStmt.executeUpdate("PREPARE TRANSACTION '" + txid + "'");
            conn.commit();

            LOG.info("Prepared Transaction {}", txid);
        } catch (SQLException e) {
            hasPendingCommitTxLock.unlock();
            throw new Exception("Could not prepare transaction (txid='" + txid + "') for commit", e);
        }

        // At this point, the commit must be guaranteed to succeed eventually

        // After preCommit(), beginTransaction() will be called immediately to start a new TX.
    }

    @Override
    protected void commit(String txid) {
        // In general, commit() should never fail (as preCommit must ensure commit succeeds)
        try {
            try (Statement txStmt = conn.createStatement()) {
                // COMMIT PREPARED cannot be run within a transaction block (after BEGIN), so we have to enable auto-commit
                conn.setAutoCommit(true);

                txStmt.executeUpdate("COMMIT PREPARED '" + txid + "'");
            } finally {
                // Make sure we reset auto-commit to off for next flushes/invokes
                conn.setAutoCommit(false);

                // We can now allow early flushes of batches again
                hasPendingCommitTxLock.unlock();
            }
        } catch (SQLException sqlex) {
            // A database access error may have occurred, but at this point we're screwed and there's no way
            // around potential data loss anymore.
            // This case is also possible if the Flink app crashes for some other reason right after calling our
            // commit() (which will execute COMMIT PREPARED), but before removing the just comitted txid from state.
            // Then, on recover, Flink wants us to commit that txid again, but it is already comitted. For this reason,
            // we only log that case and continue.
            LOG.error("FATAL Database access error during commit", sqlex);
        }
    }

    /**
     * Called for each preCommit()-ed transaction that has been recovered after a failure
     */
    @Override
    protected void recoverAndCommit(String txid) {
        // recoverAndCommit() may be called before open(), since it is called in Flink internal initializeState()
        try {
            if (!isConnected())
                reconnect();
        } catch (Exception e) {
            LOG.error("Could not reconnect to commit after recover", e);

            // We can only continue with commit if we managed to reconnect
            throw new RuntimeException("Could not reconnect to commit after recover", e);
        }

        // In case of recover, the operator may have restarted, so we have to re-acquire the lock just like
        // we would in preCommit(). It will then be unlocked in commit().
        hasPendingCommitTxLock.lock();

        commit(txid);
    }

    /**
     * abort() is called when the Operator stops ordinarily, i.e. in close().
     * It is only called on the "current" transaction, which is not yet preCommit()-ed.
     */
    @Override
    protected void abort(String txid) {
        try {
            // Rollback unfinished and un-preCommit()-ed transaction
            conn.rollback();
        } catch (SQLException e) {
            LOG.error("Could not roll back unprepared transaction", e);
        }
    }

    /**
     * This is called on the last recovered transaction that was not yet preCommit()-ed and is supposed to
     * rollback/abort this transaction.
     */
    @Override
    protected void recoverAndAbort(String txid) {
        try {
            if (!isConnected())
                reconnect();
        } catch (Exception e) {
            LOG.error("Could not reconnect to abort after recover", e);

            // We can only continue with commit if we managed to reconnect
            throw new RuntimeException("Could not reconnect to abort after recover", e);
        }

        // Most of the time, the transaction is not pre-committed yet (i.e. PREPARE TRANSACTION never completed)
        // on that txid. In this case, we don't have to do anything, since the JDBC transaction will be rolled back
        // during shutdown before recover.

        // However, it is possible that the Flink Job has to be forcefully stopped *during* the pre-commit of this
        // transaction (i.e. when PREPARE TRANSACTION takes longer than the configured Checkpoint timeout). In that
        // case, the transaction may eventually have completed prepare in the background. So we have to check that
        // and rollback that transaction in Postgres if it is already prepared.

        try {
            try (Statement txStmt = conn.createStatement()) {
                conn.setAutoCommit(true);

                ResultSet result = txStmt.executeQuery("SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid = '" + txid + "'");
                if (result.next()) {
                    if (result.getInt(1) > 0) {
                        txStmt.executeUpdate("ROLLBACK PREPARED '" + txid + "'");
                        LOG.info("Rolled backed already prepared recovered current pending transaction '{}'", txid);
                    }
                }
            } finally {
                conn.setAutoCommit(false);
            }
        } catch (SQLException sqlex) {
            LOG.error("Could not check if recovered current pending transaction is already prepared.", sqlex);
        }
    }

    @Override
    public void close() throws Exception {
        // This will call abort() on the current un-preCommit()-ed transaction.
        // TODO: abort() is not called on preCommit()-ed transactions, which were not yet commit()-ed.
        //  If the flink job does not properly recover it's state later, these Prepared Transactions will be forgotten.
        //  This is may be an implementation error in the TwoPhaseCommitSinkFunction in Flink.
        super.close();

        if (preparedStmt != null)
            preparedStmt.close();
        preparedStmt = null;

        if (conn != null)
            conn.close();
        conn = null;
    }
}
