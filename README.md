# Nap - NUMA-Aware Persistent Indexes
--------
## Evaluation Environment
We use a 4-socket server (equipped with Optane DC Persistent Memory) to evaluate Nap, which you can log into.
Please check https://osdi21ae.usenix.hotcrp.com/ for the access method of our server.

The detailed information of the server:
- 4 * 18-core Intel Xeon Gold 6240M CPUs
- 12 * 128GB Optane DIMMs 
- 12 * 32GB DDR4 DIMMs
- Ubuntu 18.04 with Linux kernel version 5.4.0

You can run ``ipmctl show -dimm`` and ``ndctl list`` to show the configuration of Optane DC PM.

The server has installed dependencies of Nap, including PMDK, tbb and google-perftools.


# Before the start
After logging into our server, run ``cd /home/wq/Nap``. This is path of our codebases.

> Note: 
> 1) Before evaluating Nap, please run ``w`` to check if anyone else is using it,
to avoid resource contention. And please close ssh connection when not conducting evalation.
> 2) If you find the performance is abnormal, please run ``reboot`` to reboot this server,
since Optane DIMMs may become slower when being writeen continuously.
> 3) If you have any question, please contact with me via q-wang18@mails.tsinghua.edu.cn

Directory Organization:
- ``include``, ``src``: codes of Nap and PM indexes
- ``bench``: codes of benchmarks
- ``script``: script for AE
- ``dataset``: dataset for evalutaion


# Functionality of codes
- ``include/cn_view.h``:   GV-View in Section 3.3
- ``include/sp_view.h``:   PC-View in Section 3.4
- ``include/top_k.h``, ``include/count_min_sketch.h``: min heap, count-min sketch and logic of hot set identification (Section 3.5)
- ``include/nap.h``: main logic of Nap, function ``nap_shift`` is 3-phase switch (Section 3.6)
- ``include/index/*``: PM indexes from https://github.com/chenzhangyu/Clevel-Hashing/ and https://github.com/utsaslab/RECIPE/
- ``bench/*_nap.cpp``: Nap-converted PM indexes.


# Main Figures
First, run ``cd script``.

## Figure 1
``bash ./run_moti_fig1.sh``, which prints the bandwith of local/remote read/write, with varying thread counts.
About 10~20 mins.

> Note: when finishing figure 1, run ``bash ./setup_eval.sh`` to initialize
ext4-DAX file systems for PMDK, which is used by remaining figures.

## Figure 8
execute ``bash ./run_fig8_{cceh, clevel, clht, masstree, fastfair}.sh``, which produces four output files:
``XX_WI_Raw, XX_RI_Nap, XX_WI_Raw, XX_RI_Nap``, where ``XX`` is {cceh, clevel, clht, masstree, fastfair}.

Each script needs 90-120 mins to run (fastfair needs > 200mins).

> Note: FastFair always triggers segment fault, so you may be run it multiple times.

> Note: All output files are saved in /home/wq/Nap/build (i.e., ../build)

> Note: If you want to complie the codes manually, please run ``rm CMakeCache.txt`` 
before run ``cmake ..``

# Other Figures

If you are interested in reproducing other figures, go ahead :)

## Figure 2(b)

This result is the same as Figure 8(a).

## Figure 3

``bash ./run_fig3.sh``, which prints the resulted access ratio.

## Figure 9

``bash ./run_fig9.sh``, producing two output files: Fig9_scan_Raw and Fig9_scan_Nap.

About 60 mins.

## Figure 10

``bash ./run_fig10.sh``, which produces two output files: Fig10_lat_Raw and  Fig10_lat_Nap.
Each line of these files contains two value: ``< latency (us), CDF >``.

About 10 mins.

## Figure 11

We use Intel’s PCM tools to measure the remote PM accesses.
The pcm.x sub-tool provides the amount of data through
UPI links and the pcm-numa.x sub-tool monitors remote
DRAM accesses. Leveraging the two sub-tools, we calculate
the remote PM accesses of P-CLHT under write-intensive
workloads.

This expriment is time-consuming, since we need to run ``build/clht_nap`` multiple times to get stable results.


## Figure 12

``bash ./run_fig12.sh``, which produces two output files: Fig12_3_phase and Fig12_global_lock.
Each line of these files contains two value: ``< time (ms), throughput (ops/ms) >``.
You need to select a continuous piece of data in the output files to depict figures.

About 10-20 mins.

## Figure 13

``bash ./run_fig13.sh``, which produces two output files: Fig13_NR_WI and Fig13_NR_RI.
These files contain the results of NR under write-intensive (WR) and read-intensive (RI) workloads.

About 60 mins.

## Figure 14

``bash ./run_fig14.sh``, which produces 5 output files:
- ``Fig14_hotset_Nap``: Figure 14(a)
- ``Fig14_keyspace_Nap`` and ``Fig14_keyspace_Raw``: Figure 14(b)
- ``Fig14_zipfan_Nap`` and ``Fig14_zipfan_Raw``: Figure 14(c)

For Figure(d) and (e), since we must add/remove Optane DIMMs manually, 
we omit their evalation. But if you are interested, you can run
``bash ./run_fig8_clht.sh`` on your own machine.

About 40-60 mins.

## Table 2

``bash ./run_table2.sh``, which produces output file Table2_recovery.

> Note: FastFair always triggers segment fault, so you may be run it multiple times.

About 20 mins.

## Figure 15

We cannot provide environment of this experiment currently, 
since the client servers equipped with ConnectX-6 NICs are being occupied by others (for conducting experiments
for other papers).