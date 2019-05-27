package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import se.sics.kompics.PortType;

public class AtomicRegister extends PortType {{
  request(AR_Read_Request.class);
  request(AR_Write_Request.class);
  indication(AR_Read_Response.class);
  indication(AR_Write_Response.class);
}}
