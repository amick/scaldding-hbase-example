package com.aca

import com.twitter.scalding._
import cascading.tuple.Fields
import parallelai.spyglass.hbase._

class Job1 (args: Args) extends Job(args) {
	// read the input file
  	val pipe = TextLine( args("input") )
	// split each line of the input file using any whitespace "\\s+" as the separator
	.flatMap('line -> 'WORD) { line : String =>
		line.toLowerCase.split("\\s+")
	}
	// get count of each word in the input file
	.groupBy('WORD) { _.size }
	// convert to uppercase for use with HBase Phoenix
	.rename('size -> 'SIZE)
	// create a new pipe with byte values rather than string/number values
	val pipe2 = new HBasePipeWrapper(pipe).toBytesWritable(new Fields("WORD"), new Fields("SIZE"))
	// write to the TEST table with WORD as the rowkey, CF as the columnFamily and SIZE as the only value column
	.write(new HBaseSource("TEST", "myserver.mydomain.com:2181", 'WORD, List("CF"), List(new Fields("SIZE"))) )
}
