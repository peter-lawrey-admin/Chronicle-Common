/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.common.nio;


import net.openhft.lang.Maths;

import java.nio.channels.SelectionKey;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Fix handling of HashSet for {@link java.nio.channels.Selector}
 *
 * <p>References:
 *
 * <p><a href="https://github.com/real-logic/Aeron/blob/master/aeron-driver/src/main/java/uk/co/real_logic/aeron/driver/NioSelectedKeySet.java">Aeron - SelectionKey</a>
 * <p><a href="https://github.com/netty/netty/blob/master/transport/src/main/java/io/netty/channel/nio/SelectedSelectionKeySet.java">Netty - SelectionKey</a>
 */
public class VanillaSelectionKeySet extends AbstractSet<SelectionKey> {
    public static final int NB_KEYS_SMALL = 16;
    public static final int NB_KEYS_BIG   = 1024;

    private SelectionKey[] keys;
    private int size;

    public VanillaSelectionKeySet() {
       this(NB_KEYS_SMALL);
    }

    public VanillaSelectionKeySet(final int capacity) {
        this.keys = new SelectionKey[Maths.nextPower2(NB_KEYS_SMALL, capacity)];
        this.size = 0;

        Arrays.fill(this.keys, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(final SelectionKey key) {
        if (null == key) {
            return false;
        }

        ensureCapacity(size + 1);
        keys[size++] = key;

        return true;
    }

    /**
     * Reset for next iteration.
     */
    @Override
    public void clear() {
        for (int i = 0; i < keys.length && keys[i] != null; i++) {
            keys[i] = null;
        }

        size = 0;
    }

    /**
     * Return selected keys.
     *
     * @return selected keys
     */
    public SelectionKey[] keys() {
        return keys;
    }

    /**
     * Capacity of the current set
     *
     * @return capacity of the set
     */
    public int capacity() {
        return keys.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final Object o) {
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * It seems that on MacOSX this method is used to check if a SelectionKey has
     * already been added to the list of availables keys.
     */
    @Override
    public boolean contains(final Object o) {
        if(o instanceof SelectionKey) {
            final SelectionKey key = (SelectionKey)o;
            for (int i = 0; i < keys.length && keys[i] != null; i++) {
                if(keys[i] == key) {
                    return true;
                }

                if(keys[i].channel() == key.channel() && keys[i].interestOps() == key.interestOps()) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<SelectionKey> iterator() {
        throw new UnsupportedOperationException();
    }

    private void ensureCapacity(final int requiredCapacity) {
        if (requiredCapacity < 0) {
            final String s = String.format("Insufficient capacity: length=%d required=%d", keys.length, requiredCapacity);
            throw new IllegalStateException(s);
        }

        if (requiredCapacity > keys.length) {
            final int newCapacity = Maths.nextPower2(keys.length, requiredCapacity);
            keys = Arrays.copyOf(keys, newCapacity);
        }
    }
}
