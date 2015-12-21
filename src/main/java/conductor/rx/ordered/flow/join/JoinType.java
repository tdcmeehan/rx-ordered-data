package conductor.rx.ordered.flow.join;

/**
 * Supported join types
 *
 * @author jrosenblum
 *
 */
public enum JoinType {

    /**
     * When a join of this type occurs, only those records with matching keys in the two upstream result sets will be
     * included.
     */
    FULL_INNER(ZipType.INNER),
    /**
     * When a join of this type occurs, every record in either of the upstream result sets will be included. Where there
     * are matching keys, all the fields present in either of the records will be present on the joined downstream
     * record. For a given key, if one side or the other of the join does not have a record with that key, then all the
     * columns on that "missing" side will be null in the downstream result set.
     */
    FULL_OUTER(ZipType.OUTER),
    /**
     * When a join of this type occurs, every record in the left upstream result set will be included. If there is a
     * matching result for a given key in the right upstream result set, its values will be merged in; otherwise all the
     * columns corresponding to the right result set will be null in the joined downstream result set for that key.
     */
    LEFT(ZipType.LEFT);
    private final ZipType zipType;

    private JoinType(final ZipType zipType) {
        this.zipType = zipType;
    }

    public final ZipType toZipType() {
        return this.zipType;
    }
}
