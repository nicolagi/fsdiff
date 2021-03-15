package main

import "log"

func logDebug(x interface{}, y ...interface{}) {
	if len(y) == 0 {
		log.Printf("DEBUG: %v", x)
	}
	if format, ok := x.(string); ok {
		log.Printf("DEBUG: "+format, y...)
	} else {
		log.Printf("DEBUG: *BROKEN CALL*: %v", x)
	}
}

func logError(x interface{}, y ...interface{}) {
	if len(y) == 0 {
		log.Printf("ERROR: %v", x)
	}
	if format, ok := x.(string); ok {
		log.Printf("ERROR: "+format, y...)
	} else {
		log.Printf("ERROR: *BROKEN CALL*: %v", x)
	}
}

func logFatal(x interface{}, y ...interface{}) {
	if len(y) == 0 {
		log.Fatalf("FATAL: %v", x)
	}
	if format, ok := x.(string); ok {
		log.Fatalf("FATAL: "+format, y...)
	} else {
		log.Fatalf("FATAL: *BROKEN CALL*: %v", x)
	}
}

func logInfo(x interface{}, y ...interface{}) {
	if len(y) == 0 {
		log.Printf("INFO: %v", x)
	}
	if format, ok := x.(string); ok {
		log.Printf("INFO: "+format, y...)
	} else {
		log.Printf("INFO: *BROKEN CALL*: %v", x)
	}
}

func logWarn(x interface{}, y ...interface{}) {
	if len(y) == 0 {
		log.Printf("WARNING: %v", x)
	}
	if format, ok := x.(string); ok {
		log.Printf("WARNING: "+format, y...)
	} else {
		log.Printf("WARNING: *BROKEN CALL*: %v", x)
	}
}
