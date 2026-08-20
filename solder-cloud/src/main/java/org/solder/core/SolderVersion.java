package org.solder.core;

public class SolderVersion {
	
	
	
	static final String POM_VERSION="4.2.1";
	static final int  BUILD_NUMBER = 101;
	static final String BUILD_DATE  = "08-20-2026 12:25";
	
	public static final String VERSION = String.format("Solder %s %03d; %s",POM_VERSION,BUILD_NUMBER,BUILD_DATE);
	
	

}