Instance
--------

Instance Type: c5.metal


lscpu
-----
Architecture:        x86_64
CPU op-mode(s):      32-bit, 64-bit
Byte Order:          Little Endian
CPU(s):              72
On-line CPU(s) list: 0-71
Thread(s) per core:  2
Core(s) per socket:  18
Socket(s):           2
NUMA node(s):        2
Vendor ID:           GenuineIntel
CPU family:          6
Model:               85
Model name:          Intel(R) Xeon(R) Platinum 8124M CPU @ 3.00GHz
Stepping:            4
CPU MHz:             1200.837
CPU max MHz:         3500.0000
CPU min MHz:         1200.0000
BogoMIPS:            6000.00
Virtualization:      VT-x
L1d cache:           32K
L1i cache:           32K
L2 cache:            1024K
L3 cache:            25344K
NUMA node0 CPU(s):   0-17,36-53
NUMA node1 CPU(s):   18-35,54-71
Flags:               fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx pdpe1gb rdtscp lm constant_tsc art arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc cpuid aperfmperf pni pclmulqdq dtes64 monitor ds_cpl vmx smx est tm2 ssse3 sdbg fma cx16 xtpr pdcm pcid dca sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand lahf_lm abm 3dnowprefetch cpuid_fault epb cat_l3 cdp_l3 invpcid_single pti intel_ppin mba ibrs ibpb stibp tpr_shadow vnmi flexpriority ept vpid fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm cqm mpx rdt_a avx512f avx512dq rdseed adx smap clflushopt clwb intel_pt avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves cqm_llc cqm_occup_llc cqm_mbm_total cqm_mbm_local dtherm ida arat pln pts hwp hwp_act_window hwp_epp hwp_pkg_req pku ospke



DONE vs. TODO
-------------

### APSP

- ACTIX (MISSING/SKIP)
- AKKA (run-1571440753749)
- AKKATYPED (run-1571440753749)
- ERLANG (run-1571905502266, INCOMPLETE)
- KOMPACTAC (run-1571835417152)
- KOMPACTCO (run-1571348723734 with general CPU) (run-1571835417152)
- KOMPICSJ (run-1571769649675)
- KOMPICSSC (run-1571440753749)
- KOMPICSSC2 (run-1571744204926)
- RIKER (run-1571348723734 with general CPU)

### ATOMICREGISTER

- AKKA (run-1572009419324)
- AKKATYPED (run-1572009419324)
- ERLANG (run-1572009419324)
- KOMPACTAC (run-1572009419324)
- KOMPACTMIX (run-1572009419324)
- KOMPICSJ (run-1572009419324)
- KOMPICSSC (run-1572009419324)
- KOMPICSSC2 (run-1572009419324)

### Chameneos

- ACTIX (MISSING/SKIP)
- AKKA (run-1571440753749)
- AKKATYPED (run-1571440753749)
- ERLANG (run-1571937811971)
- KOMPACTAC (run-1571835417152)
- KOMPACTMIX (run-1571835417152)
- KOMPICSJ (run-1571769649675, INCOMPLETE) (run-1572015926167)
- KOMPICSSC (run-1571440753749)
- KOMPICSSC2 (run-1571744204926)
- RIKER (run-1571348723734 with general CPU)

### Fibonacci

- ACTIX (run-1572339302256)
- AKKA (run-1571440753749)
- AKKATYPED (run-1571440753749)
- ERLANG (run-1571937811971)
- KOMPACTAC (run-1571835417152)
- KOMPICSJ (run-1572339302256)
- KOMPICSSC (run-1571440753749)
- KOMPICSSC2 (run-1571744204926)
- RIKER (run-1571348723734 with general CPU)

### NETPINPONG

- AKKA (run-1571440753749)
- AKKATYPED (run-1571440753749)
- ERLANG (run-1571937811971)
- KOMPACTAC (run-1571835417152)
- KOMPICSJ (run-1572041118592)
- KOMPICSSC (run-1572041118592)
- KOMPICSSC2 (run-1572041118592)

### NETTPPINGPONG

- AKKA (run-1571440753749)
- AKKATYPED (run-1571440753749)
- ERLANG (run-1571937811971)
- KOMPACTAC (run-1571835417152)
- KOMPICSJ (run-1572041118592)
- KOMPICSSC (run-1572041118592)
- KOMPICSSC2 (run-1572041118592)

### PINGPONG

- ACTIX (run-1572164105219)
- AKKA (run-1571440753749)
- AKKATYPED (run-1571440753749)
- ERLANG (run-1571937811971)
- KOMPACTAC (run-1571835417152)
- KOMPACTCO (run-1571348723734 with general CPU) (run-1571835417152)
- KOMPICSJ (run-1572164105219)
- KOMPICSSC (run-1572164105219)
- KOMPICSSC2 (run-1572164105219)
- RIKER (run-1572164105219)

### STREAMINGWINDOWS

- AKKA (run-1571440753749)
- AKKATYPED (run-1571440753749)
- ERLANG (run-1571937811971)
- KOMPACTAC (run-1571835417152)
- KOMPICSJ (run-1572041118592)
- KOMPICSSC (run-1571440753749, INCOMPLETE) (run-1572041118592)
- KOMPICSSC2 (run-1571744204926, INCOMPLETE) (run-1572041118592)

### TPPINGPONG

- ACTIX (MISSING/SKIP)
- AKKA (run-1571440753749)
- AKKATYPED (run-1571440753749)
- ERLANG (run-1571937811971)
- KOMPACTAC (run-1571835417152)
- KOMPACTCO (run-1571348723734 with general CPU) (run-1571835417152)
- KOMPICSJ (run-1572218619357)
- KOMPICSSC (run-1572218619357)
- KOMPICSSC2 (run-1572218619357)
- RIKER (run-1571348723734 with general CPU, INCOMPLETE)





