package com.qst.mapreduce;

import java.util.StringTokenizer;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
	
	String str = "hello world tom\r\nhello tom world\r\nhello tomson" ;
	@Test
	public void testhash(){
		Integer t = -128;
		System.out.println(t.hashCode());
	    System.out.println("tom".hashCode());
	    System.out.println("hello".hashCode());
	    System.out.println("world".hashCode());
	    
	    StringTokenizer st = new StringTokenizer(str, "[ \n\r]");
	    while(st.hasMoreTokens()){
	    	String word = st.nextToken();
	    	System.out.println(word);
	    	
	    }
	}
	@Test
	public void tset2(){
		System.out.println( 2 / 0 );
	}
	
	
}
    