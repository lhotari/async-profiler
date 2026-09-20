/*
 * Copyright The async-profiler authors
 * SPDX-License-Identifier: Apache-2.0
 */

package test.signal;

import one.profiler.test.Assert;
import one.profiler.test.Os;
import one.profiler.test.Output;
import one.profiler.test.Test;
import one.profiler.test.TestProcess;
import test.cpu.CpuBurner;

public class SignalTests {
    @Test(mainClass = CpuBurner.class, os = Os.LINUX, runIsolated = true)
    public void externalSignalsRespectInterval(TestProcess p) throws Exception {
        p.profile("start -e signal -i 100ms");

        for (int i = 0; i < 20; i++) {
            Process signal = new ProcessBuilder("kill", "-PROF", Long.toString(p.pid())).start();
            Assert.isEqual(signal.waitFor(), 0, "sending SIGPROF should succeed");
            Thread.sleep(20);
        }

        Output out = p.profile("stop -o collapsed");
        Assert.isGreaterOrEqual(out.total(), 2, "external signals should produce samples");
        Assert.isLessOrEqual(out.total(), 6, "the interval should limit accepted samples");
    }

    @Test(mainClass = CpuBurner.class, os = Os.LINUX, runIsolated = true)
    public void signalStreamCanRestartWhileSignalsRaceWithStop(TestProcess p) throws Exception {
        for (int round = 0; round < 10; round++) {
            p.profile("start -e signal -i 1ns -o jsonl -f %f.jsonl");

            Process signals = new ProcessBuilder(
                    "sh", "-c",
                    "i=0; while [ $i -lt 200 ]; do kill -PROF " + p.pid()
                            + " || exit; i=$((i + 1)); done")
                    .start();
            p.profile("stop");
            Assert.isEqual(signals.waitFor(), 0, "signals racing with stop should not terminate the JVM");

            Process lateSignal = new ProcessBuilder("kill", "-PROF", Long.toString(p.pid())).start();
            Assert.isEqual(lateSignal.waitFor(), 0, "a late pending signal should be safely ignored");
            p.profile("status");
        }
    }
}
