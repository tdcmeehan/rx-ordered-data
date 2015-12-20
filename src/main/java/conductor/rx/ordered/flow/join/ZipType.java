package conductor.rx.ordered.flow.join;

/**
 * Supported join types.  This is different from {@link conductor.core.io.streaming.processors.flow.join.JoinType} in that these do not represent full SQL joins, but
 * item-by-item joining of data.  Implicit is that keys are unique in each stream; if the keys are not unique, then the
 * behavior is undefined.  Use constructs that employ {@link conductor.core.io.streaming.processors.flow.join.JoinType} if you anticipate duplicate keys or choose not
 * to aggregate them before hand.
 */
public enum ZipType {
    /**
     * Entries must exist on both sides in order to be processed
     */
    INNER,
    /**
     * Entries must exist on the left side and optionally be present on the right side
     */
    LEFT,
    /**
     * Entries can exist on either side and will be processed regardless
     */
    OUTER
}
