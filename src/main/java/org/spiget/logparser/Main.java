package org.spiget.logparser;

import java.io.IOException;
import java.text.ParseException;

public class Main {

	public static void main(String[] args) throws IOException, ParseException {
		SpigetLogParser parser = new SpigetLogParser();
		parser.init();
		parser.parse();
	}

}
