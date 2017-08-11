package org.broadinstitute.hellbender.tools.spark.sv.discovery;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@DefaultSerializer(AlignmentStrand.Serializer.class)
public enum AlignmentStrand {
    FORWARD, REVERSE;

    public void serialize(final Kryo kryo, final Output output) {
        output.write(this.ordinal());
    }

    @Override
    public String toString() {
        return this.equals(AlignmentStrand.FORWARD) ? "+" : "-";
    }

    public static final class Serializer extends com.esotericsoftware.kryo.Serializer<AlignmentStrand>{
        @Override
        public void write(final Kryo kryo, final Output output, final AlignmentStrand strand) {
            strand.serialize(kryo, output);
        }

        @Override
        public AlignmentStrand read(final Kryo kryo, final Input input, final Class<AlignmentStrand> kclass) {
            return AlignmentStrand.values()[input.readInt()];
        }
    }
}
