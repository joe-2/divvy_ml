import logging
import time

def initLog(cfg):
	logger = logging.getLogger(cfg.get('logging', 'logName'))
	logPath=cfg.get('logging', 'logPath')
	logFilename=cfg.get('logging', 'logFileName')  
	hdlr = logging.FileHandler(logPath+time.strftime("%Y%m%d%H%M%S")+logFilename)
	formatter = logging.Formatter(cfg.get('logging', 'logFormat'),cfg.get('logging', 'logTimeFormat'))
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr) 
	logger.setLevel(logging.INFO)
	return logger
