/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

final class EncoderArrayList extends ArrayList<Object> {

    private static final long serialVersionUID = -8605125654176467947L;

    private static final int DEFAULT_INITIAL_CAPACITY = 8;

    private static final Recycler<EncoderArrayList> RECYCLER = new Recycler<EncoderArrayList>() {
        @Override
        protected EncoderArrayList newObject(Handle handle) {
            return new EncoderArrayList(handle);
        }
    };

    static EncoderArrayList newInstance(int mergeThreshold) {
        EncoderArrayList ret = RECYCLER.get();
        ret.mergeThreshold = mergeThreshold;
        ret.ensureCapacity(DEFAULT_INITIAL_CAPACITY);
        return ret;
    }

    private final Handle handle;
    private int mergeThreshold;
    private EncoderArrayList(Handle handle) {
        this(handle, DEFAULT_INITIAL_CAPACITY);
    }

    private EncoderArrayList(Handle handle, int initialCapacity) {
        super(initialCapacity);
        this.handle = handle;
    }

    @Override
    public boolean addAll(Collection<?> c) {
        checkNullElements(c);
        return super.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<?> c) {
        checkNullElements(c);
        return super.addAll(index, c);
    }

    private static void checkNullElements(Collection<?> c) {
        if (c instanceof RandomAccess && c instanceof List) {
            // produce less garbage
            List<?> list = (List<?>) c;
            int size = list.size();
            for (int i = 0; i  < size; i++) {
                if (list.get(i) == null) {
                    throw new IllegalArgumentException("c contains null values");
                }
            }
        } else {
            for (Object element: c) {
                if (element == null) {
                    throw new IllegalArgumentException("c contains null values");
                }
            }
        }
    }

    @Override
    public boolean add(Object element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        int size = size();
        if (size > 0 && element instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) element;
            if (buf.readableBytes() < mergeThreshold) {
                Object last = get(size - 1);
                if (last instanceof ByteBuf) {
                    ByteBuf lastBuf = (ByteBuf) last;
                    if (lastBuf.writableBytes() >= buf.readableBytes()) {
                        lastBuf.writeBytes(buf);
                        buf.release();
                        return true;
                    }
                }
            }
        }
        return super.add(element);
    }

    @Override
    public void add(int index, Object element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        super.add(index, element);
    }

    @Override
    public Object set(int index, Object element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        return super.set(index, element);
    }

    /**
     * Clear and recycle this instance.
     */
    public boolean recycle() {
        clear();
        return RECYCLER.recycle(this, handle);
    }
}
