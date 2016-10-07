package com.isentropy.accumulo.experimental.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.io.LongBinarySerde;
import com.isentropy.accumulo.experimental.collections.ScriptEnabledMap;
import com.isentropy.accumulo.experimental.mappers.JavaScriptMappers;
import com.isentropy.accumulo.util.Util;

import static com.isentropy.accumulo.experimental.mappers.JavaScriptMappers.jsFilter;
import static com.isentropy.accumulo.experimental.mappers.JavaScriptMappers.jsTransform;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class ExperimentalFeaturesTest 
extends TestCase
{
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public ExperimentalFeaturesTest()
	{
		super( "testApp" );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite( ExperimentalFeaturesTest.class );
	}

	public void testApp(){
		try{
			Connector c = new MockInstance().getConnector("root", new PasswordToken());
	    	//set up map load [x,2*x] for x in 1 to 1000
		/*
			AccumuloSortedMap asm = new AccumuloSortedMap(c,"mytable");
	    	asm.setKeySerde(new LongBinarySerde()).setValueSerde(new DoubleBinarySerde());
			for(long i=0;i<1000;i++){
				asm.put(i, 2*i);
			}
			asm.subMap(100, 200).sample(.5).deriveMap(jsFilter("k % 2 == 0")).deriveMap(jsTransform("v*5")).dump(System.out);
		*/	
/*		
			AccumuloSortedMap mapOfLongToMap = new AccumuloSortedMap(c,"mytable2");
			for(long x=0;x<10;x++){
				Map m = new HashMap();
				m.put("twox", 2*x);
				m.put("threex", 3*x);
				mapOfLongToMap.put(x,m);
			}
			JavaScriptMappers.JavaScriptFilterMapper jm = new JavaScriptMappers.JavaScriptFilterMapper("k + v['twox'] + v['threex']",null,true);
			// this is a map of [x,6x]
			mapOfLongToMap.deriveMap(jsTransform("k + v['twox'] + v['threex']")).dump(System.out);;
			System.out.println("checksum = " + mapOfLongToMap.checksum());
*/			
			ScriptEnabledMap mapOfLongToMap = new ScriptEnabledMap(c,"mytable2");
			for(long x=0;x<10;x++){
				String json = "{\"a\":"+(x+1)+"}";
				mapOfLongToMap.put(x,json);
			}
			// this is a map of [x,6x]
			AccumuloSortedMap ja = mapOfLongToMap.jsTransform("j['a']",true);
			ja.dump(System.out);
			mapOfLongToMap.jsFilter("k % 2 == 0", false).dump(System.out);
			System.out.println("checksum = " + mapOfLongToMap.checksum());
		}
		
		catch(Exception e){
			e.printStackTrace();
			fail();
		}

	}
}

