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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.AccumuloSortedMapBase;
import com.isentropy.accumulo.collections.AccumuloSortedMultiMap;
import com.isentropy.accumulo.collections.AccumuloSortedProperties;
import com.isentropy.accumulo.collections.JoinRow;
import com.isentropy.accumulo.collections.Link;
import com.isentropy.accumulo.collections.MapAggregates;
import com.isentropy.accumulo.collections.factory.AccumuloSortedMapFactory;
import com.isentropy.accumulo.collections.io.DoubleBinarySerde;
import com.isentropy.accumulo.collections.io.LongAsUtf8Serde;
import com.isentropy.accumulo.collections.io.LongBinarySerde;
import com.isentropy.accumulo.collections.transform.KeyValueTransformer;
import com.isentropy.accumulo.util.TsvInputStreamIterator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AccumuloMapTest 
extends TestCase
{
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public AccumuloMapTest( String testName )
	{
		super( testName );
	}

	public void testImport(Connector c) throws AccumuloException, AccumuloSecurityException, IOException{
		String contents = "a\taa\n"+
				"b\tbb\n"+
				"c\tcc\n";
				
		AccumuloSortedProperties im = new AccumuloSortedProperties(c,"testimport");
		im.importAll(new TsvInputStreamIterator(new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8))));
		assertTrue(im.containsKey("a"));
		assertTrue(im.containsValue("bb"));
		assertTrue(im.size() == 3);
		im.dump(System.out);
		
		
	}
	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite( AccumuloMapTest.class );
	}

	public void demo() throws AccumuloException, AccumuloSecurityException, InterruptedException{
		//connect to accumulo
		//Connector c = new ZooKeeperInstance(instanceName,zookeepers).getConnector(myUsername, new PasswordToken(myPassword));
		Connector c = new MockInstance().getConnector("root", new PasswordToken());
		//set up map load [x,2*x] for x in 1 to 1000
		AccumuloSortedMap asm = new AccumuloSortedMap(c,"mytable");
		asm.setKeySerde(new LongBinarySerde()).setValueSerde(new LongBinarySerde());
		for(long i=0;i<1000;i++){
			asm.put(i, 2*i);
		}


		// MAP FEATURES
		System.out.println("get(100) = "+asm.get(100));
		// SORTED MAP FEATURES
		AccumuloSortedMapBase submap = (AccumuloSortedMapBase) asm.subMap(10, 20).subMap(11, 30);
		submap.dump(System.out);

		// should be null
		System.out.println("submap(10,20).get(100) = "+submap.get(100));
		//should be 10
		System.out.println("submap(10,20).firstKey() = "+submap.firstKey());


		System.out.println("sample: "+asm.sample(.5).subMap(0, 100).size());


		// SAMPLING
		// sample 50% of submap and print key values
		submap.sample(.5).dump(System.out); System.out.println();
		// this should be a DIFFERENT set because the seed is random in both calls to sample()
		submap.sample(.5).dump(System.out); System.out.println();

		// sample 40% of entries based on the ordering of seed "randomseed"
		submap.sample(0,.4,"randomseed").dump(System.out); System.out.println();
		// should be the SAME set as above if map hasn't changed because seed and hash range are the same
		submap.sample(0,.4,"randomseed").dump(System.out); System.out.println();

		// SERVER-SIDE aggregates
		//numerical stats work for values that are instances of Number
		System.out.println("submap value mean = "+MapAggregates.valueStats(submap).getMean());
		//size() is also computed on tablet servers
		System.out.println("submap size = "+submap.size());

		//TIMEOUT features
		asm.setTimeOutMs(5000);
		Thread.sleep(3000);
		asm.put(123, 345);
		Thread.sleep(2000);
		//all entries except 123 will have timed out by now. size should be 1
		System.out.println("map size after sleep = "+asm.size());

	}

	private void printCollections(AccumuloSortedMapBase map){
		Iterator it = map.entrySet().iterator();
		for(;it.hasNext();){
			Entry e = (Entry) it.next();
			System.out.println("k = "+ e.getKey() + ", v = "+e.getValue());
		}
		it= map.keySet().iterator();
		for(;it.hasNext();){
			System.out.println("k = "+ it.next());
		}
		it= map.values().iterator();
		for(;it.hasNext();){
			System.out.println("v = "+ it.next());
		}
		StatisticalSummary ssmStats = MapAggregates.valueStats(map);
		System.out.println("map stats:\n"+ssmStats);


	}
	
	public void testMapFactory(Connector c) throws AccumuloException, AccumuloSecurityException, InstantiationException, IllegalAccessException, ClassNotFoundException, TableNotFoundException{
		AccumuloSortedMapFactory fact = new AccumuloSortedMapFactory(c,"factory_table");
		String tableName = "test_map_factory";
		AccumuloSortedMap asm = fact.makeMap(tableName);
		AccumuloSortedMap asm2 = new AccumuloSortedMap(c,asm.getTable());
		boolean err = false;
		try{
			asm.setKeySerde(new LongBinarySerde());
		}
		catch(Exception e){
			err=true;
		}
		assertTrue(err);
		asm.put(123, 456);
		assertTrue(asm2.get(123).equals(456));

		asm.clear();
		/*
		//change the default serde to LongBinarySerde
		fact.addDefaultProperty(AccumuloSortedMapFactory.MAP_PROPERTY_KEY_SERDE, LongBinarySerde.class.getName());
		fact.addDefaultProperty(AccumuloSortedMapFactory.MAP_PROPERTY_VALUE_SERDE, LongBinarySerde.class.getName());
		asm = fact.makeMap(tableName);
		asm.put(123, 4567);
		asm2.setKeySerde(new LongBinarySerde()).setValueSerde(new LongBinarySerde());
		assertTrue(asm2.get(123).equals(4567l));
		
		asm.clear();
		*/
		
		//change the table-specific metadata for this table
		fact.addMapSpecificProperty(tableName, AccumuloSortedMapFactory.MAP_PROPERTY_KEY_SERDE, LongAsUtf8Serde.class.getName());
		asm = fact.makeMap(tableName);
		asm.put(123, 456);
		asm2.setKeySerde(new LongAsUtf8Serde());
		assertTrue(asm2.get(123).equals(456));
		
		//asm.delete();
	}
	public void testMultiMap(Connector c) throws AccumuloException, AccumuloSecurityException{
		AccumuloSortedMultiMap mm = new AccumuloSortedMultiMap(c,"mm");
		mm.put(1, 2);
		mm.put(1, 3);
		mm.put(1, 4);
		mm.put(2, 22);
		SummaryStatistics row1=(SummaryStatistics) mm.rowStats().get(1);
		assertTrue(row1.getMean()==3.0);
		assertTrue(row1.getMax()==4.0);
		// size should reflect # keys
		assertTrue(mm.size()==2);
		
		
	}
	public void testLinks(Connector c) throws AccumuloException, AccumuloSecurityException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		
		AccumuloSortedMapFactory fact = new AccumuloSortedMapFactory(c,"factory_table2");
		AccumuloSortedMap m1 = fact.makeMap("m1");
		AccumuloSortedMap m2 = fact.makeMap("m2");
		
		m1.put("a", "b");
		Link l = m1.makeLink("a");
		m2.put("aa", l);
		assertTrue(m2.getResolvedLink("aa").equals("b"));
	}

	public void testApp()
	{
		try{
			Connector c = new MockInstance().getConnector("root", new PasswordToken());
			testLinks(c);
			testMapFactory(c);
			testMultiMap(c);
/*
    		//setup for MiniAccumulo
			File tempDirectory = new File("/tmp/asmTest");
			MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDirectory, "password");
			accumulo.start();
			Thread.sleep(60000);
			Connector c = accumulo.getConnector("root", "password");
*/			
			// run benchmark:
	//		new Benchmark().benchmark(c, System.out, 999999,10);
			
			//set up map load [x,2*x] for x in 1 to 1000
			AccumuloSortedMap asm = new AccumuloSortedMap(c,"mytable");
			
			boolean err = false;
			try{
				AccumuloSortedMap asmSameNameConflict = new AccumuloSortedMap(c,"mytable",true,true);
			}
			catch(AccumuloException e){
				err = true;
			}
			assertTrue(err);
			
			
			err=false;
			try{
				AccumuloSortedMap asmSameNameNoConflict = new AccumuloSortedMap(c,"mytable",true,false);
			}
			catch(AccumuloException e){
				err = true;
			}
			assertFalse(err);
			
			err=false;
			try{
				AccumuloSortedMap readOnly = new AccumuloSortedMap(c,"mytable",true,false);
				readOnly.setReadOnly(true);
				readOnly.put(123, 456);
			}
			catch(UnsupportedOperationException e){
				err = true;
			}
			assertTrue(err);
			
			long preaddts = System.currentTimeMillis();
			asm.setKeySerde(new LongBinarySerde()).setValueSerde(new LongBinarySerde());
			for(long i=0;i<1000;i++){
				asm.put(i, 2*i);
			}
			long postaddts = System.currentTimeMillis();
			long ts123 = asm.getTimestamp(123);
		
			//test join
			/*
			int i=0;
			for(Iterator<JoinRow> join = asm.joinOnValue(asm);join.hasNext()&&i++<20;){
				JoinRow row = join.next();
				System.out.println(row);
				assertEquals(row.getTransformedKey(),row.getValue());
				assertEquals(2*((Long) row.getValue()),row.getJoinValue(0));
				
			}
			*/
			
			
			//sample random range
			Random rand = new Random();
			double r1=rand.nextDouble(),r2=rand.nextDouble();
			double from = Math.min(r1, r2);
			double to = Math.max(r1, r2);
			
			long checksum = asm.sample(from,to,"abc").checksum();
			//verfiy sample is the same
			assertTrue(checksum == asm.sample(from,to,"abc").checksum());
			System.out.println("checksum = " + checksum);
			
			
			
			System.out.println("ts123 = " + new Date(ts123));
			assertTrue(ts123 >= preaddts && ts123 <= postaddts);
			assertTrue(asm.timeFilter(0,preaddts-1).size() == 0);
			assertTrue(asm.timeFilter(preaddts,postaddts).size() == 1000);
			assertTrue(asm.timeFilter(postaddts+1,Long.MAX_VALUE).size() == 0);
			
			assertTrue(asm.containsKey(123l));
			assertFalse(asm.containsKey(-123l));
			assertNull(asm.get(-1));
			assertTrue(asm.keySet().contains(123l));
			assertTrue(asm.firstKey().equals(0l));
			assertTrue(asm.get(100).equals(200l));
			assertTrue(asm.size() == 1000);
			assertTrue(asm.values().contains(1002l));
			assertFalse(asm.values().contains(1001l));
			int hs = asm.headMap(123).size();
			assertTrue(asm.headMap(123).size() == 123);
			assertTrue(asm.tailMap(123).size() == 1000 -123);
			
			/*
			 * for(Object o : asm.keySet()){
				System.out.println("ks "+o);
			}
			*/
			assertTrue(asm.keySet().contains(999l));
			assertFalse(asm.keySet().contains(1001l));

			AccumuloSortedMap copyOfAsm = new AccumuloSortedMap(c,"othertable");
			copyOfAsm.setKeySerde(new LongBinarySerde()).setValueSerde(new LongBinarySerde());
			copyOfAsm.putAll(asm);
			int sz = asm.size();
			assertTrue(copyOfAsm.size() == sz);
			Object o = copyOfAsm.get(101l);
			assertTrue(copyOfAsm.get(101l).equals(202l));
			HashMap toAdd = new HashMap();
			for(int x=1000;x<1010;x++)
				toAdd.put(x, 3*x);
			copyOfAsm.putAll(toAdd);
			assertTrue(copyOfAsm.size() == sz+10);
			copyOfAsm.remove(0l);
			assertTrue(copyOfAsm.size() == sz+9);
			copyOfAsm.clear();
			assertTrue(copyOfAsm.size() == 0);
			copyOfAsm.importAll(new ImportSimulationIterator(10));
			assertTrue(copyOfAsm.size() == 10);
			assertTrue(copyOfAsm.get(1l).equals(2l));
			copyOfAsm.dump(System.out);


			AccumuloSortedMap transformedCopyOfAsm = new AccumuloSortedMap(c,"transformed");
			transformedCopyOfAsm.setKeySerde(new LongBinarySerde()).setValueSerde(new LongBinarySerde());
			transformedCopyOfAsm.putAll(asm, new KeyValueTransformer(){
				@Override
				public Entry transformKeyValue(Object fk, Object fv) {
					// invert key and value
					return new AbstractMap.SimpleEntry(fv,fk);
				}
			}
			);
			assertTrue(transformedCopyOfAsm.get(202l).equals(101l));
			assertTrue(transformedCopyOfAsm.size() == sz);
			System.out.println("Inverted map: ");
			transformedCopyOfAsm.subMap(10, 20).dump(System.out);
			

			testImport(c);

			//asm.subMap(0, 5).dump(System.out);


			AccumuloSortedMapBase submap = (AccumuloSortedMapBase) asm.subMap(10, 20);
			err = false;
			try{
				submap.put(1, -1);
			}
			catch(UnsupportedOperationException e){
				err = true;
			}
			// put should throw err in submap
			assertTrue(err);
			submap = submap.subMap(11, 33);
			assertTrue(submap.size() == 9);
			assertTrue(submap.get(11).equals(asm.get(11)));
			assertTrue(submap.get(12).equals(asm.get(12)));
			assertTrue(submap.lastKey().equals(19l));
			
			//System.out.println("submap.get(11) = "+submap.get(11));
			//System.out.println("asm.get(11) = "+asm.get(11));

			System.out.println("submap(10,20).submap(11,33)");
			printCollections(submap);
			System.out.println("submap(10,20).submap(11,33).sample(.5)");
			printCollections(submap.sample(.5));


			AccumuloSortedMapBase sample = asm.sample(.5);
			System.out.println("sample(.5).sample(.1)");
			printCollections(sample.sample(.1));

			asm.setTimeOutMs(5000);
			Thread.sleep(3000);
			asm.put(123, 345);
			Thread.sleep(2000);
			//all entries except 123 will have timed out by now. size should be 1
			System.out.println("map size after sleep = "+asm.size());
			assertTrue(asm.size() == 1);

		}
		catch(Exception e){
			e.printStackTrace();
			fail();
		}
	}
}
