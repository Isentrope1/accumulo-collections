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

package com.isentropy.accumulo.test;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.io.LongBinarySerde;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class Benchmark {
	byte[] cf = "cf".getBytes();
	byte[] cq = "cq".getBytes();
	byte[] cv = new byte[0];
	Authorizations auths = new Authorizations(cv);
	ColumnVisibility cvis = new ColumnVisibility(cv);

	protected Entry<Key, Value> getEntry(Scanner s, byte[] keyBytes) {
		Key k1 = new Key(keyBytes,cf,cq,cv,0);
		Key k2 = new Key(keyBytes,cf,cq,cv,Long.MAX_VALUE);
		s.setRange(new Range(k2,k1));
		Iterator<Entry<Key, Value>> it = s.iterator();
		if(it.hasNext()){
			Entry<Key, Value> e = it.next();
			return e;
		}
		return null;
	}
	protected Timer getTimer(MetricRegistry reg, String name){
		SortedMap<String,Timer> timers = reg.getTimers();
		Timer t;
		if(timers == null || (t=timers.get(name)) == null){
			t =new Timer();
			reg.register(name, t);
			return t;
		}
		return t;
	}
	/**
	 * 
	 * @param c
	 * @throws TableNotFoundException 
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 */
	public void benchmark(Connector c, PrintStream ps, long numInsertsPerTest, int numTests) throws TableNotFoundException, AccumuloException, AccumuloSecurityException{
		MetricRegistry metrics = new MetricRegistry();
		ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS).outputTo(ps)
				.build();

		for(int testNum =0;testNum<numTests;testNum++){

			String rawTable = "rawTable";
			try {
				c.tableOperations().create(rawTable);
			}
			catch (TableExistsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			String testname = "raw_write_batched";
			Timer t = getTimer(metrics,testname);
			BatchWriter bw = c.createBatchWriter(rawTable, new BatchWriterConfig());
			Timer.Context context = t.time();
			for(long l=0;l<numInsertsPerTest;l++){
				Mutation m = new Mutation(LongBinarySerde.longSerialize(l));
				m.put(cf,cq,cvis,LongBinarySerde.longSerialize(2*l));
				bw.addMutation(m);		
			}
			bw.flush();
			bw.close();
			context.stop();

			testname = "raw_read";
			t = getTimer(metrics,testname);
			context = t.time();
			Scanner s = c.createScanner(rawTable, auths);
			for(long l=0;l<numInsertsPerTest;l++){
				Entry e = getEntry(s,LongBinarySerde.longSerialize(l));
				if(e == null)
					Assert.fail("consistency error in raw Accumulo write benchmark");
			}
			context.stop();

			c.tableOperations().delete(rawTable);




			String rawTable2 = "rawTable2";
			try {
				c.tableOperations().create(rawTable2);
			}
			catch (TableExistsException e) {
				e.printStackTrace();
			}
			testname = "raw_write_unbatched";
			t = getTimer(metrics,testname);
			
			context = t.time();
			BatchWriter bw2 = c.createBatchWriter(rawTable2, new BatchWriterConfig());
			for(long l=0;l<numInsertsPerTest;l++){
				Mutation m = new Mutation(LongBinarySerde.longSerialize(l));
				m.put(cf,cq,cvis,LongBinarySerde.longSerialize(2*l));
				bw2.addMutation(m);		
				bw2.flush();
			}
			bw2.close();
			context.stop();
			c.tableOperations().delete(rawTable2);


			testname = "AccumuloSortedMap_write_batched";
			t = getTimer(metrics,testname);
			context = t.time();
			AccumuloSortedMap asm = new AccumuloSortedMap(c,"asm1");
			asm.setKeySerde(new LongBinarySerde()).setValueSerde(new LongBinarySerde());
			asm.importAll(new ImportSimulationIterator(numInsertsPerTest));
			context.stop();
			asm.delete();

			testname = "AccumuloSortedMap_write_unbatched";
			t = getTimer(metrics,testname);
			context = t.time();
			asm = new AccumuloSortedMap(c,"asm2");
			asm.setKeySerde(new LongBinarySerde()).setValueSerde(new LongBinarySerde());
			for(long l=0;l<numInsertsPerTest;l++){
				asm.putWithoutGet(l,l+1);
			}
			context.stop();

			testname = "AccumuloSortedMap_read";
			t = getTimer(metrics,testname);
			context = t.time();
			for(long l=0;l<numInsertsPerTest;l++){
				Object v = asm.get(l);
				if(v == null)
					Assert.fail("consistency error in AccumuloSortedMap benchmark");
			}
			context.stop();


			asm.delete();

		}


		reporter.report();
	}

	public Benchmark() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
