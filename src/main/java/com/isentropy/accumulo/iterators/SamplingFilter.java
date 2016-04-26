/* 
Accumulo Collections
Copyright 2016 Isentropy LLC
Written by Jonathan Wolff <jwolff@isentropy.com>
Isentropy specializes in big data and quantitative programming consulting,
with particular expertise in Accumulo development and installation. 
More info at http://isentropy.com.


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.isentropy.accumulo.iterators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.OptionDescriber.IteratorOptions;
import org.apache.hadoop.io.BytesWritable;

import com.isentropy.accumulo.util.Util;

public class SamplingFilter extends Filter{
	public static final String OPT_FROMFRACTION = "fromfrac";
	public static final String OPT_TOFRACTION = "tofrac";
	public static final String OPT_RANDOMSEED = "seed";
	public static final String OPT_MAXTIMESTAMP = "maxts";
	
	private byte[] fromHash,toHash,randSeed;
	private double fromfrac=0,tofrac=1;
	private String mdtype = "SHA-256";
	private long maxts = -1;
	
	BytesWritable.Comparator comp = new BytesWritable.Comparator();
	MessageDigest md;

	private int compare(byte[] a,byte[] b){
//		int cmp = comp.compare(a, 0, a.length, b, 0, b.length);
		int cmp = new BytesWritable(a).compareTo(new BytesWritable(b));
		//System.out.println(Util.bytesToHex(a).substring(0, 4)+" vs "+Util.bytesToHex(b).substring(0, 4) + " = "+cmp);
		return cmp;
	}
	
	@Override
	public IteratorOptions describeOptions() {
		IteratorOptions opts = super.describeOptions();
		opts.addNamedOption(OPT_FROMFRACTION, "from hash fraction of entries to sample");
		opts.addNamedOption(OPT_TOFRACTION, "to hash fraction of entries to sample");
		opts.addNamedOption(OPT_RANDOMSEED, "rand bytes to be added to hashed bytes");
		opts.addNamedOption(OPT_MAXTIMESTAMP, "maximum timestamp to accept (optional)");
		return opts;
	}

	@Override
	public boolean validateOptions(Map<String,String> options) {
		if(!super.validateOptions(options))
			return false;
		return options.containsKey(OPT_FROMFRACTION) && 
				options.containsKey(OPT_TOFRACTION) &&
				options.containsKey(OPT_RANDOMSEED);
	}
	
	protected byte[] hash(byte[] b, byte[] randseed){
		md.reset();
		md.update(randseed);
		md.update(b);
		byte[] h = md.digest();
		//System.out.println(Util.bytesToHex(h));
		return h;
	}
	
	@Override
	public boolean accept(Key k, Value v) {
		if(maxts > 0 && maxts < k.getTimestamp())
			return false;
		byte[] kb = k.getRowData().toArray();
		byte[] kbhash = hash(kb,randSeed);
		int cmpto = compare(kbhash,toHash), cmpfrom = compare(kbhash,fromHash);
		return cmpfrom >=0 && (cmpto < 0 || (tofrac == 1 && cmpto ==0 ));
	}
	
	@Override
	public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
		super.init(source,options,env);
		try {
			md = MessageDigest.getInstance(mdtype);
		} catch (NoSuchAlgorithmException e) {
			throw new IOException(e);
		}
		fromfrac = Double.parseDouble(options.get(OPT_FROMFRACTION));
		tofrac = Double.parseDouble(options.get(OPT_TOFRACTION));
		fromHash = Util.hashPoint(md.getDigestLength(), fromfrac);
		toHash = Util.hashPoint(md.getDigestLength(), tofrac);
		randSeed =  options.get(OPT_RANDOMSEED).getBytes(StandardCharsets.UTF_8);
		String tso = options.get(OPT_MAXTIMESTAMP);
		
		if(tso != null){
			maxts = Integer.parseInt(tso);
		}
		
		//System.out.println("from: "+Util.bytesToHex(fromHash));
		//System.out.println("to: " + Util.bytesToHex(toHash));
		
	}
}
