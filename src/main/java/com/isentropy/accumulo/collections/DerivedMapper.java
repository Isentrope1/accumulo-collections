package com.isentropy.accumulo.collections;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.isentropy.accumulo.collections.io.SerDe;

/**
 *  DerivedMapper wraps an iterator with options and a SerDe to use for the derived map
 */

public interface DerivedMapper {
	public  Class<? extends SortedKeyValueIterator<Key, Value>> getIterator();
	public Map<String,String> getIteratorOptions();
	public SerDe getDerivedMapValueSerde();
}
