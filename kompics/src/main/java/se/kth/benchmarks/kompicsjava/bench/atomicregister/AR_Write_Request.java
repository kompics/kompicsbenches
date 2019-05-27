package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import se.sics.kompics.KompicsEvent;

public class AR_Write_Request implements KompicsEvent {
  int value;

  public AR_Write_Request(int value){
    this.value = value;
  }
}
