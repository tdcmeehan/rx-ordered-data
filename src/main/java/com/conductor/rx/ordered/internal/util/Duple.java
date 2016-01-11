/* (C) Copyright 2009, Conductor. All rights reserved. */
package com.conductor.rx.ordered.internal.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * A simple data structure that holds two objects with checked types.
 *
 * @param <L>
 *            the type for the &quot;left-hand&quot; part of the duple
 * @param <R>
 *            the type for the &quot;right-hand&quot; part of the duple
 */
public final class Duple<L, R> {
    private final L leftMember;
    private final  R rightMember;

    public Duple(final L fst, final R snd) {
        leftMember = fst;
        rightMember = snd;
    }

    /**
     * Returns the object stored in the &quot;left-hand&quot; side of the duple.
     *
     * @return the &quot;left-hand&quot; object
     */
    public final L getFst() {
        return leftMember;
    }

    /**
     * Returns the object stored in the &quot;right-hand&quot; side of the duple.
     * 
     * @return the &quot;right-hand&quot; object
     */
    public final R getSnd() {
        return rightMember;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Duple that = (Duple) o;

        return Objects.equal(this.leftMember, that.leftMember) &&
                Objects.equal(this.rightMember, that.rightMember);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(leftMember, rightMember);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("leftMember", leftMember)
                .add("rightMember", rightMember)
                .toString();
    }
}
