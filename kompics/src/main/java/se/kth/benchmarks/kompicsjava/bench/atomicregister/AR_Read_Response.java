package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import se.sics.kompics.KompicsEvent;

public class AR_Read_Response implements KompicsEvent {
  int value;

  public AR_Read_Response(int value){
    this.value = value;
  }
}
