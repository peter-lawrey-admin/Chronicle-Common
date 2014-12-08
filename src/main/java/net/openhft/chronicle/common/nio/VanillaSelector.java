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

import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.Set;

/**
 * Wrapper for {@link java.nio.channels.Selector}.
 *
 * <p> Examples of usage:
 *
 * <blockquote>
 * <pre>
 *     final ServerSocketChannel server = ServerSocketChannel.open();
 *     server.configureBlocking(false);
 *     server.bind(new InetSocketAddress("localhost", 9876));
 *
 *     final VanillaSelector selector = new VanillaSelector();
 *     selector.open();
 *     selector.register(server, SelectionKey.OP_ACCEPT);
 *
 *     for(int i=0; i<1000; i++) {
 *         selector.selectAndProcess((final SelectionKey key) ->
 *             if(key.isAcceptable()) {
 *                 startClient(
 *                     ((ServerSocketChannel)key.channel()).accept()
 *                 );
 *             }
 *         );
 *     }
 * </pre>
 * </blockquote>
 */
public class VanillaSelector implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaSelector.class);
    private static final int DEFAULT_SELECT_SPIN_COUNT = 10000;
    private static final long DEFAULT_SELECT_TIMEOUT = 0L;

    private VanillaSelectionKeySet selectionKeySet;
    private Selector selector;
    private SelectorProcessor processor;

    public VanillaSelector() {
        this.selectionKeySet = null;
        this.selector = null;
        this.processor = null;
    }

    public VanillaSelector open() throws IOException {
        this.selector = Selector.open();

        if(!Boolean.getBoolean("chronicle.nio.useNioSelector")) {
            try {
                final ClassLoader loader = ClassLoader.getSystemClassLoader();
                final Class<?> clazz = Class.forName("sun.nio.ch.SelectorImpl", false, loader);

                if (clazz.isAssignableFrom(Selector.open().getClass())) {
                    this.selectionKeySet = new VanillaSelectionKeySet();
                    this.processor = new VanillaSelectorProcessor();

                    final Field selectedKeysField = clazz.getDeclaredField("selectedKeys");
                    selectedKeysField.setAccessible(true);

                    final Field publicSelectedKeysField = clazz.getDeclaredField("publicSelectedKeys");
                    publicSelectedKeysField.setAccessible(true);

                    selectedKeysField.set(this.selector, this.selectionKeySet);
                    publicSelectedKeysField.set(this.selector, this.selectionKeySet);
                }
            } catch (Exception e) {
                this.selectionKeySet = null;
                this.processor = null;

                LOGGER.error("", e);
            }
        }

        if(this.processor == null) {
            this.processor = new NioSelectorProcessor();
        }

        return this;
    }

    public VanillaSelector register(@NotNull AbstractSelectableChannel channel, int ops) throws IOException {
        channel.register(this.selector, ops);
        return this;
    }

    public VanillaSelector deregister(@NotNull AbstractSelectableChannel channel, int ops) throws IOException {
        SelectionKey selectionKey = channel.keyFor(this.selector);
        if (selectionKey != null) {
            selectionKey.interestOps(selectionKey.interestOps() & ~ops);
        }

        return this;
    }

    public VanillaSelectionKeySet vanillaSelectionKeys() {
        return this.selectionKeySet;
    }

    public Set<SelectionKey> selectionKeys() {
        return selector.selectedKeys();
    }

    public int select(int spinLoopCount, long timeout) throws IOException {
        for (int i = 0; i < spinLoopCount; i++) {
            int nbKeys = selector.selectNow();
            if(nbKeys != 0) {
                return nbKeys;
            }
        }

        return selector.select(timeout);
    }

    public int selectAndProcess(final VanillaSelectionKeyHandler handler) throws IOException {
        return selectAndProcess(DEFAULT_SELECT_SPIN_COUNT, DEFAULT_SELECT_TIMEOUT, false, handler);
    }

    public int selectAndProcess(int spinLoopCount, long timeout, final VanillaSelectionKeyHandler handler) throws IOException {
        return selectAndProcess(spinLoopCount, timeout, false, handler);
    }

    public int selectAndProcess(boolean failOnError, final VanillaSelectionKeyHandler handler) throws IOException {
        return selectAndProcess(DEFAULT_SELECT_SPIN_COUNT, DEFAULT_SELECT_TIMEOUT, failOnError, handler);
    }

    public int selectAndProcess(int spinLoopCount, long timeout, boolean failOnError, final VanillaSelectionKeyHandler handler) throws IOException {
        int handledKeys = 0;
        if(select(spinLoopCount, timeout) > 0) {
            handledKeys = process(handler, failOnError);
        }

        return handledKeys;
    }

    public int process(final VanillaSelectionKeyHandler handler) {
        return process(handler, false);
    }

    public int process(final VanillaSelectionKeyHandler handler, boolean stopOnError) {
        return processor.process(handler, stopOnError);
    }

    @Override
    public void close() throws IOException {
        if(selector != null) {
            selector.close();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    private interface SelectorProcessor {
        public int process(final VanillaSelectionKeyHandler handler, boolean stopOnError);
    }

    private final class VanillaSelectorProcessor implements SelectorProcessor {
        @Override
        public int process(final VanillaSelectionKeyHandler handler, boolean stopOnError) {
            final int selectedKeys = selectionKeySet.size();
            final SelectionKey[] selectionKyes = selectionKeySet.keys();

            int handledKeys = 0;
            while(handledKeys < selectedKeys) {
                try {
                    handler.onKey(selectionKyes[handledKeys]);
                } catch(IOException e) {
                    LOGGER.warn("", e);

                    if(stopOnError) {
                        break;
                    }
                }

                handledKeys++;
            }

            selectionKeySet.clear();

            return handledKeys;
        }
    }

    private final class NioSelectorProcessor implements SelectorProcessor {
        @Override
        public int process(final VanillaSelectionKeyHandler handler, boolean stopOnError) {
            final Set<SelectionKey> keys = selector.selectedKeys();

            int handledKeys = 0;
            for(final SelectionKey key : keys) {
                try {
                    handler.onKey(key);
                } catch(IOException e) {
                    LOGGER.warn("", e);

                    if(stopOnError) {
                        break;
                    }
                }

                handledKeys++;
            }

            keys.clear();

            return handledKeys;
        }
    }
}
