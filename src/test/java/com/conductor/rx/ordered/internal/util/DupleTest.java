package com.conductor.rx.ordered.internal.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DupleTest {

    @Test
    public void testGetFst() throws Exception {
        assertEquals("1", new Duple<>("1", "2").getFst());
    }

    @Test
    public void testGetSnd() throws Exception {
        assertEquals("2", new Duple<>("1", "2").getSnd());
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(new Duple<>("1", "2"), new Duple<>("1", "2"));
    }

    @Test
    public void testHashCode() throws Exception {
        assertEquals(2530, new Duple<>("1", "2").hashCode());
    }

    @Test
    public void testToString() throws Exception {
        assertEquals("Duple{leftMember=1, rightMember=2}", new Duple<>("1", "2").toString());
    }
}