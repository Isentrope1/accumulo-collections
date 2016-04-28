package com.isentropy.accumulo.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.OptionDescriber.IteratorOptions;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.io.SerDe;

public abstract class DeserializedTransformingIterator extends TransformingIterator implements OptionDescriber{
	public static Logger log = LoggerFactory.getLogger(DeserializedTransformingIterator.class);
	public static final String OPT_KEYSERDE = "keyserde";
	public static final String OPT_VALUE_INPUT_SERDE = "value_input_serde";
	public static final String OPT_VALUE_OUTPUT_SERDE = "value_output_serde";
	protected SerDe key_serde = null;
	protected SerDe value_input_serde = null;
	protected SerDe value_output_serde = null;
	
	
	@Override
	public IteratorOptions describeOptions() {
		String desc = "An iterator that wraps a AccumuloMapFilter to filter bytes on the tablet server";
		HashMap<String,String> namedOptions = new HashMap<String,String>();
		namedOptions.put(OPT_KEYSERDE, "serde for input keys");
		namedOptions.put(OPT_VALUE_INPUT_SERDE, "serde for input values");
		namedOptions.put(OPT_VALUE_OUTPUT_SERDE, "serde for output values");
		return new IteratorOptions(getClass().getSimpleName(), desc, namedOptions, null);
	}

	@Override
	public boolean validateOptions(Map<String,String> options) {
		String o;
		if((o = options.get(OPT_KEYSERDE)) != null){
			try{
				SerDe s= (SerDe) Class.forName(o).newInstance();
			}
			catch(Exception e){
				log.error("Error instantiating SerDe "+o+": "+e.getMessage());
				return false;
			}
		}
		if((o = options.get(OPT_VALUE_INPUT_SERDE)) != null){
			try{
				SerDe s= (SerDe) Class.forName(o).newInstance();
			}
			catch(Exception e){
				log.error("Error instantiating SerDe "+o+": "+e.getMessage());
				return false;
			}
		}
		if((o = options.get(OPT_VALUE_OUTPUT_SERDE)) != null){
			try{
				SerDe s= (SerDe) Class.forName(o).newInstance();
			}
			catch(Exception e){
				log.error("Error instantiating SerDe "+o+": "+e.getMessage());
				return false;
			}
		}
		return true;
	}
	@Override
	public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
		super.init(source,options,env);
		String o;
		try{
			if((o = options.get(OPT_KEYSERDE)) != null)
				key_serde = (SerDe) Class.forName(o).newInstance();
			if((o = options.get(OPT_VALUE_INPUT_SERDE)) != null)
				value_input_serde = (SerDe) Class.forName(o).newInstance();
			if((o = options.get(OPT_VALUE_OUTPUT_SERDE)) != null)
				value_output_serde = (SerDe) Class.forName(o).newInstance();
		}
		catch(Exception e){
			throw new IOException(e);
		}
	}
	
	
	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW;
	}
	
	protected abstract Object transform(Object key,Object value) throws IOException;
	

	@Override
	protected final void transformRange(SortedKeyValueIterator<Key, Value> input,
			KVBuffer output) throws IOException {
		while(input.hasTop()){
			Key k = input.getTopKey();
			byte[] kd = k.getRowData().toArray();
			Value v = input.getTopValue();
			Object vo = value_input_serde.deserialize(v.get());
			Object ko = key_serde.deserialize(kd);
			output.append(k, new Value(value_output_serde.serialize(transform(ko,vo))));
			input.next();
		}
	}

}
