package org.apache.ignite.internal.processors.cache;

import org.junit.Test;

import static org.junit.Assert.*;

public class GridCacheMvccCandidateEqualsTest {
    @Test
    public void equalsTest(){
        GridCacheMvccCandidate obj = new GridCacheMvccCandidate();
        assertFalse(obj.equals(new Object()));
    }
}
