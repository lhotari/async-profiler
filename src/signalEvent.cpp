/*
 * Copyright The async-profiler authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include "event.h"
#include "log.h"
#include "os.h"
#include "profiler.h"
#include "signalEvent.h"
#include "tsc.h"
#include "vmEntry.h"
#include "writer.h"


struct StreamSample {
    u64 timestamp;
    u64 monotonic_timestamp_ns;
    u64 trace;
};

int SignalEvent::_signal;
int SignalEvent::_pipe[2] = {-1, -1};
int SignalEvent::_output = -1;
pthread_t SignalEvent::_writer_thread;
SigAction SignalEvent::_previous_handler;
long SignalEvent::_interval;
volatile u64 SignalEvent::_last_sample;
volatile u64 SignalEvent::_failed_traces;
volatile u64 SignalEvent::_dropped_samples;

void SignalEvent::signalHandler(int signo, siginfo_t* siginfo, void* ucontext) {
    int saved_errno = errno;
    if (!_enabled) {
        errno = saved_errno;
        return;
    }

    u64 now = OS::nanotime();
    u64 last_sample = __atomic_load_n(&_last_sample, __ATOMIC_RELAXED);
    while (true) {
        if (now - last_sample < (u64)_interval) {
            errno = saved_errno;
            return;
        }
        if (__atomic_compare_exchange_n(&_last_sample, &last_sample, now, false,
                                        __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
            break;
        }
    }

    ExecutionEvent execution_event(TSC::ticks());
    u64 trace = Profiler::instance()->recordSample(ucontext, 1, EXECUTION_SAMPLE, &execution_event);
    if (trace == 0) {
        __atomic_add_fetch(&_failed_traces, 1, __ATOMIC_RELAXED);
    } else if (_pipe[1] >= 0) {
        StreamSample sample = {OS::micros() / 1000, now, trace};
        // Writing a fixed-size record to a non-blocking pipe is async-signal-safe.
        // If the consumer falls behind, discard the sample instead of blocking the
        // sampled application thread in this signal handler.
        ssize_t result = write(_pipe[1], &sample, sizeof(sample));
        if (result != sizeof(sample)) {
            __atomic_add_fetch(&_dropped_samples, 1, __ATOMIC_RELAXED);
        }
    }
    errno = saved_errno;
}

void* SignalEvent::writerThreadEntry(void* unused) {
    writerLoop();
    return NULL;
}

void SignalEvent::writerLoop() {
    bool attached = VM::attachThread("Async-profiler Signal Stream") != NULL;
    FileWriter out(_output);
    _output = -1;

    StreamSample sample;
    while (true) {
        ssize_t bytes = read(_pipe[0], &sample, sizeof(sample));
        if (bytes == sizeof(sample)) {
            if (attached) {
                Profiler::instance()->writeStreamEvent(
                    out, sample.timestamp, sample.monotonic_timestamp_ns, sample.trace);
                // JSONL is a live stream consumed while profiling is active.
                out.flush();
            }
        } else if (bytes == 0) {
            break;
        } else if (bytes < 0 && errno == EINTR) {
            continue;
        } else if (bytes < 0) {
            Log::warn("Cannot read signal sample stream: %s", strerror(errno));
            break;
        }
    }

    if (attached) {
        VM::detachThread();
    }
}

Error SignalEvent::startJsonlWriter(const char* file) {
    if (file == NULL) {
        return Error("jsonl output requires an output file");
    }

    _output = open(file, O_WRONLY | O_TRUNC | O_CREAT, 0644);
    if (_output < 0) {
        return Error("Cannot open jsonl output file");
    }
    if (pipe(_pipe) != 0) {
        close(_output);
        _output = -1;
        return Error("Cannot create signal sample pipe");
    }

    int flags = fcntl(_pipe[1], F_GETFL);
    if (fcntl(_pipe[0], F_SETFD, FD_CLOEXEC) != 0 ||
            fcntl(_pipe[1], F_SETFD, FD_CLOEXEC) != 0 ||
            flags < 0 || fcntl(_pipe[1], F_SETFL, flags | O_NONBLOCK) != 0) {
        close(_pipe[0]);
        close(_pipe[1]);
        close(_output);
        _pipe[0] = _pipe[1] = _output = -1;
        return Error("Cannot configure signal sample pipe");
    }
    if (pthread_create(&_writer_thread, NULL, writerThreadEntry, NULL) != 0) {
        close(_pipe[0]);
        close(_pipe[1]);
        close(_output);
        _pipe[0] = _pipe[1] = _output = -1;
        return Error("Cannot start signal sample writer");
    }
    return Error::OK;
}

void SignalEvent::stopJsonlWriter() {
    close(_pipe[1]);
    _pipe[1] = -1;
    pthread_join(_writer_thread, NULL);
    close(_pipe[0]);
    _pipe[0] = -1;
}

Error SignalEvent::start(Arguments& args) {
    if (!VM::loaded()) {
        return Error("signal event requires a JVM");
    }

    if (args._interval < 0) {
        return Error("interval must be positive");
    }
    _interval = args._interval ? args._interval : DEFAULT_INTERVAL;
    _last_sample = 0;
    _failed_traces = 0;
    _dropped_samples = 0;

    if (args._output == OUTPUT_JSONL) {
        Error error = startJsonlWriter(args.file());
        if (error) return error;
    }

    _signal = args._signal == 0 ? SIGPROF : args._signal & 0xff;
    _previous_handler = OS::installSignalHandler(_signal, signalHandler);
    return Error::OK;
}

void SignalEvent::stop() {
    OS::installSignalHandler(_signal, _previous_handler);
    if (_pipe[1] >= 0) {
        stopJsonlWriter();
    }
    u64 failed_traces = __atomic_load_n(&_failed_traces, __ATOMIC_RELAXED);
    u64 dropped_samples = __atomic_load_n(&_dropped_samples, __ATOMIC_RELAXED);
    if (failed_traces != 0) {
        Log::warn("Signal event failed to obtain %llu stack traces", failed_traces);
    }
    if (dropped_samples != 0) {
        Log::warn("Signal event dropped %llu samples from the output stream", dropped_samples);
    }
}
