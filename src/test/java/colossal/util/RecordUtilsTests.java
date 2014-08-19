/**
 * Copyright (C) 2010-2014 Think Big Analytics, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package colossal.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class RecordUtilsTests {

    private static class SubBase {
        protected int inner;
    }
    private static class Base extends SubBase {
        public transient String x;
        private Long y;
        static int z;
    }
    static class Super extends Base {
        public int ignore;
    }
    
    Base a, b;
    Super c;

    @Before
    public void setup() {
        a = new Base();
        b = new Base();
        c = new Super();
        a.x = "a";
        b.x = "b";
        c.x = "c";
        a.y = 11L;
        c.ignore = 12;
    }
    
    @Test
    public void copySameClass() {
        RecordUtils.copy(a, b);
        assertEquals("a", b.x);
        assertEquals(new Long(11), b.y);
    }
    
    
    @Test
    public void copySuperClass() {
        RecordUtils.copy(c, a);
        assertEquals("c", a.x);
        assertNull(a.y);
    }
    
    @Test
    public void copySubClass() {
        RecordUtils.copy(a, c);
        assertEquals("a", c.x);
        assertEquals(new Long(11), ((Base)c).y);
    }

}
