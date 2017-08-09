package org.broadinstitute.hellbender.tools.spark.sv.utils;

import java.io.Serializable;
import java.util.function.Predicate;

@FunctionalInterface
public interface SerializablePredicate<T extends Object> extends Predicate<T>, Serializable {
    static final long serialVersionUID = 1L;
}
