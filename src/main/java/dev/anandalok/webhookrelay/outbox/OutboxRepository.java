package dev.anandalok.webhookrelay.outbox;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Lock;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {

    /**
     * FOR UPDATE SKIP LOCKED is what makes this safe to run on more than one instance.
     * Two pollers grabbing the same batch would double-publish; SKIP LOCKED lets the
     * second one step over rows the first is already holding instead of blocking on them.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")}) // -2 = SKIP_LOCKED
    @Query("select m from OutboxMessage m where m.status = :status order by m.createdAt asc")
    List<OutboxMessage> lockPendingBatch(@Param("status") OutboxMessage.Status status, Pageable pageable);
}
