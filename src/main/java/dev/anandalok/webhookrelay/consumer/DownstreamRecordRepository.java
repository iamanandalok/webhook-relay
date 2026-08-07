package dev.anandalok.webhookrelay.consumer;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface DownstreamRecordRepository extends JpaRepository<DownstreamRecord, UUID> {
}
