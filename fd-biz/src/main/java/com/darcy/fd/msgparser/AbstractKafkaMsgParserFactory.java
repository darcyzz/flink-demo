// Copyright (C) 2019 Darcy. All rights reserved.

package com.darcy.fd.msgparser;

import java.io.Serializable;

public abstract class AbstractKafkaMsgParserFactory implements Serializable {
	public abstract FdKafkaMsgParser getMsgHandler(String kafkaMsgHandleName);
}



