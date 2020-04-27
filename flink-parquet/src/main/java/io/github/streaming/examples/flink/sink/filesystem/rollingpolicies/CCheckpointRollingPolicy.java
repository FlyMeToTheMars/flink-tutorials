package io.github.streaming.examples.flink.sink.filesystem.rollingpolicies;

import java.io.IOException;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy.PolicyBuilder;
import org.apache.flink.util.Preconditions;

/**
 * A {@link RollingPolicy} which rolls (ONLY) on every checkpoint.
 */
public final class CCheckpointRollingPolicy<IN, BucketID> extends
    CheckpointRollingPolicy<IN, BucketID> {

  private static final long serialVersionUID = 1L;

  private static final long DEFAULT_INACTIVITY_INTERVAL = 60L * 1000L;

  private static final long DEFAULT_ROLLOVER_INTERVAL = 60L * 1000L;

  private static final long DEFAULT_MAX_PART_SIZE = 1024L * 1024L * 128L;

  private final long partSize;

  private final long rolloverInterval;

  private final long inactivityInterval;

  /**
   * Private constructor to avoid direct instantiation.
   */
  private CCheckpointRollingPolicy(long partSize, long rolloverInterval, long inactivityInterval) {
    Preconditions.checkArgument(partSize > 0L);
    Preconditions.checkArgument(rolloverInterval > 0L);
    Preconditions.checkArgument(inactivityInterval > 0L);

    this.partSize = partSize;
    this.rolloverInterval = rolloverInterval;
    this.inactivityInterval = inactivityInterval;
  }

  @Override
  public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element)
      throws IOException {
    return partFileState.getSize() > partSize;
  }

  @Override
  public boolean shouldRollOnProcessingTime(PartFileInfo<BucketID> partFileState,
      long currentTime) throws IOException {
    return currentTime - partFileState.getCreationTime() >= rolloverInterval ||
        currentTime - partFileState.getLastUpdateTime() >= inactivityInterval;
  }

  /**
   * Returns the maximum part file size before rolling.
   *
   * @return Max size in bytes
   */
  public long getMaxPartSize() {
    return partSize;
  }

  /**
   * Returns the maximum time duration a part file can stay open before rolling.
   *
   * @return Time duration in milliseconds
   */
  public long getRolloverInterval() {
    return rolloverInterval;
  }

  /**
   * Returns time duration of allowed inactivity after which a part file will have to roll.
   *
   * @return Time duration in milliseconds
   */
  public long getInactivityInterval() {
    return inactivityInterval;
  }


  /**
   * Creates a new {@link DefaultRollingPolicy.PolicyBuilder} that is used to configure and build an
   * instance of {@code DefaultRollingPolicy}.
   */
  public static RollingPolicyBuilder builder() {
    return new RollingPolicyBuilder(
        DEFAULT_MAX_PART_SIZE,
        DEFAULT_ROLLOVER_INTERVAL,
        DEFAULT_INACTIVITY_INTERVAL);
  }

  public static class RollingPolicyBuilder<IN, BucketID, T extends PolicyBuilder<IN, BucketID, T>> extends
      PolicyBuilder<IN, BucketID, T> {

    private final long partSize;

    private final long rolloverInterval;

    private final long inactivityInterval;

    private RollingPolicyBuilder(
        final long partSize,
        final long rolloverInterval,
        final long inactivityInterval) {
      this.partSize = partSize;
      this.rolloverInterval = rolloverInterval;
      this.inactivityInterval = inactivityInterval;
    }

    /**
     * Sets the part size above which a part file will have to roll.
     *
     * @param size the allowed part size.
     */
    public RollingPolicyBuilder withMaxPartSize(final long size) {
      Preconditions.checkState(size > 0L);
      return new RollingPolicyBuilder(size, rolloverInterval, inactivityInterval);
    }

    /**
     * Sets the interval of allowed inactivity after which a part file will have to roll.
     *
     * @param interval the allowed inactivity interval.
     */
    public RollingPolicyBuilder withInactivityInterval(final long interval) {
      Preconditions.checkState(interval > 0L);
      return new RollingPolicyBuilder(partSize, rolloverInterval, interval);
    }

    /**
     * Sets the max time a part file can stay open before having to roll.
     *
     * @param interval the desired rollover interval.
     */
    public RollingPolicyBuilder withRolloverInterval(final long interval) {
      Preconditions.checkState(interval > 0L);
      return new RollingPolicyBuilder(partSize, interval, inactivityInterval);
    }

    @Override
    public CheckpointRollingPolicy<IN, BucketID> build() {
      return new CCheckpointRollingPolicy<>(partSize, rolloverInterval, inactivityInterval);
    }
  }

}
