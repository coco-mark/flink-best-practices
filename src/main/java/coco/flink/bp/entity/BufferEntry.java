package coco.flink.bp.entity;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Objects;

/**
 * A container for elements put in the left/write buffer. This will contain the element itself
 * along with a flag indicating if it has been joined or not.
 */
@Internal
@VisibleForTesting
public class BufferEntry<T> {

    private final T element;
    private boolean hasBeenJoined;

    public BufferEntry(T element, boolean hasBeenJoined) {
        this.element = element;
        this.hasBeenJoined = hasBeenJoined;
    }

    @VisibleForTesting
    public T getElement() {
        return element;
    }

    @VisibleForTesting
    public boolean hasBeenJoined() {
        return hasBeenJoined;
    }

    public void joined() {
        this.hasBeenJoined = true;
    }

    /**
     * A {@link TypeSerializer serializer} for the {@link BufferEntry}.
     */
    @Internal
    @VisibleForTesting
    public static class BufferEntrySerializer<T> extends TypeSerializer<BufferEntry<T>> {

        private static final long serialVersionUID = -20197698803836236L;

        private final TypeSerializer<T> elementSerializer;

        public BufferEntrySerializer(TypeSerializer<T> elementSerializer) {
            this.elementSerializer = Preconditions.checkNotNull(elementSerializer);
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<BufferEntry<T>> duplicate() {
            return new BufferEntrySerializer<>(elementSerializer.duplicate());
        }

        @Override
        public BufferEntry<T> createInstance() {
            return null;
        }

        @Override
        public BufferEntry<T> copy(BufferEntry<T> from) {
            return new BufferEntry<>(from.element, from.hasBeenJoined);
        }

        @Override
        public BufferEntry<T> copy(BufferEntry<T> from, BufferEntry<T> reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(BufferEntry<T> record, DataOutputView target) throws IOException {
            target.writeBoolean(record.hasBeenJoined);
            elementSerializer.serialize(record.element, target);
        }

        @Override
        public BufferEntry<T> deserialize(DataInputView source) throws IOException {
            boolean hasBeenJoined = source.readBoolean();
            T element = elementSerializer.deserialize(source);
            return new BufferEntry<>(element, hasBeenJoined);
        }

        @Override
        public BufferEntry<T> deserialize(BufferEntry<T> reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeBoolean(source.readBoolean());
            elementSerializer.copy(source, target);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BufferEntrySerializer<?> that = (BufferEntrySerializer<?>) o;
            return Objects.equals(elementSerializer, that.elementSerializer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(elementSerializer);
        }

        @Override
        public TypeSerializerSnapshot<BufferEntry<T>> snapshotConfiguration() {
            return new BufferEntrySerializerSnapshot<>(this);
        }
    }

    /**
     * A {@link TypeSerializerSnapshot} for {@link BufferEntrySerializer}.
     */
    public static final class BufferEntrySerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<BufferEntry<T>, BufferEntrySerializer<T>> {

        private static final int VERSION = 2;

        @SuppressWarnings({"unused", "WeakerAccess"})
        public BufferEntrySerializerSnapshot() {
            super(BufferEntrySerializer.class);
        }

        BufferEntrySerializerSnapshot(BufferEntrySerializer<T> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                BufferEntrySerializer<T> outerSerializer) {
            return new TypeSerializer[]{outerSerializer.elementSerializer};
        }

        @Override
        @SuppressWarnings("unchecked")
        protected BufferEntrySerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new BufferEntrySerializer<>((TypeSerializer<T>) nestedSerializers[0]);
        }
    }
}


